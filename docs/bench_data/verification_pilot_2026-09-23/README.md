# Verification sandbox: initial implementation evidence

The complete component experiment passed on 2026-09-23. This is **unanchored bootstrap evidence**, not whole-engine verification, completion of the roadmap's P1 concurrent-slice milestone, or approval to promote a candidate. The source remained unchanged throughout the campaign and measurement run. The exact source fingerprints identify the uncommitted implementation on top of `aa2f2c374e221dbcdd12069b91be3ed826291d98`; that Git revision alone does not contain these changes.

The [experiment receipt](experiment/receipt.json) records source identity, compiler/host configuration, timing comparisons and allocation measurements. The [verification report](experiment/verification/report.json) maps evidence to exact claim scopes and lists the six open engine obligations. [Artifact hashes](artifact_sha256.json) cover the retained machine evidence; paths in raw reports refer to their original `target/verification/experiment-final` locations.

## Reproduction and results

The complete pinned [setup workflow](setup.json) passed, including the locked Lean dependency bootstrap. The composed command was:

```sh
source target/verification-tools/environment.sh
python3 scripts/run_verified_experiment.py --bootstrap --output target/verification/experiment-final
```

The verification campaign took approximately 246 seconds on this host, followed by the component timing and allocation measurements.

| Check | Result |
| --- | --- |
| Frozen boundary | 163 protected files matched |
| Gate/adapter/model-runner tests | 15 + 5 + 9 tests passed |
| Verus | 19 production/specification obligations; six explicitly checked roots; both semantic mutants rejected |
| Lean | 14 required typed roots; complete fresh kernel replay; five Rust mutants and an axiom-free forged theorem rejected |
| Lean axioms | Only `propext`, `Classical.choice`, `Quot.sound` |
| TLA+ | All 35 declared outcomes: 12 complete searches, 10 safety counterexamples, three liveness counterexamples, 10 witnesses |
| Rust kernel tests | Three passed, including exhaustive short-input differential cases |
| Focused engine tests | 34 passed per configuration: default, insertion candidate, bitmap candidate |
| Extended Crucible | All three configurations passed 27 replay phases and all six native concurrency contracts, with eight families and four local workers |
| Allocation measurements | All 21 output/accounting/drop checks passed |

The full release workspace test suite also passed earlier in this implementation session. Its terminal output was not retained in this bundle; the composed campaign above retains its own exact logs. GitHub Actions has been configured but was not run remotely in this session.

## Initial performance observations

These are medians of nine batches of **component calls**, in nanoseconds per call. They exclude hashing, transactions, locking and durability. The comparison's standard implementation copies a slice before sorting; the existing engine path already owns its input vector. These measurements cannot establish a database speedup.

| Input case | Standard | Insertion candidate | Bitmap candidate |
| --- | ---: | ---: | ---: |
| Empty | 5.82 | 6.31 | 2,382.75 |
| Equality | 11.05 | 13.89 | 2,401.16 |
| Eight IDs | 19.13 | 29.09 | 2,508.85 |
| 64 IDs | 157.64 | 523.14 | 2,607.85 |
| 1,024 IDs, 32 distinct | 3,173.72 | 4,307.51 | 2,751.22 |
| 1,024 IDs, large key domain | 3,960.71 | 79,219.49 | 3,211.55 |
| All 4,096 buckets, already ordered | 1,905.94 | 1,051,960.50 | 4,701.34 |

The separate [allocation report](experiment/allocations.json) records requested layout bytes, not RSS or allocator metadata. For the duplicate-heavy case, standard sorting retains 8,192 bytes in the result vector; each candidate retains 256. Bitmap peak live requested storage is 4,352 bytes for that case, and 4,096 even for an empty input. Internal allocator movement during `realloc` is outside this metric. No timing executable contains this allocation instrumentation.

No algorithm was promoted. The bitmap's improvement for larger `In` inputs does not outweigh its small-input cost without a measured workload mix and end-to-end resource/latency evidence. The insertion implementation is primarily a proved alternative and comparison baseline at this stage.

## Proof boundaries

Both actual bucket bodies have independent Verus and extracted-Rust Lean proofs, including first-invalid results. Lean proves their complete result equivalence and provides corollaries for the production limit of 4,096 buckets. Allocation models, compiler/extractor correctness and platform behavior remain explicit assumptions; general logical-vector termination is not a promise of physical allocation success or absence of arbitrary-capacity panics.

The scalar stamp comparison is proved and used in production. Actual concurrent publication/history refinement, unsafe arena and guard ownership, memory-progress bounds, weak-memory/mmap semantics, durability/recovery, and query/application composition remain open. Model counterexamples to candidate durability orderings are not reproduced Rust crashes; passing alternate orderings are not implemented repairs.

See the [verification workspace](../../../verification/README.md) for supported edits, baseline anchoring and routine commands, and the [full plan](../../formal_verification_plan.md) for the remaining milestones.
