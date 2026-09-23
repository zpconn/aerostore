# Performance repair investigation

Commit `9676fa9ff602565d5a628af41b6d40ba1576a8b9` is the immutable correctness-repaired control. The [earlier campaign](../verified_engine_2026-09-23/README.md) found a 7.04% median throughput regression in the 120-second original Crucible and did **not** grant performance acceptance. Its rejected lock/layout experiments remain preserved there.

This investigation evaluates two isolated, source-motivated candidates once each: reuse WAL serialization storage while keeping its destruction outside commit guards, and reuse a removal search window while the original mutation guard remains held. Candidate implementation, validation, and exact source changes are recorded separately before timing. Neither changes the lock retry policy. A combination is considered only if the independent evidence supports its components.

The [predeclared policy](policy.json) fixes three 30-second pairs in AB/BA/AB order, the existing 16-worker/50,000-row/128 MiB fixture, and all correctness/resource requirements. Support requires at least 3% median throughput improvement, no slower individual throughput pair, conservative p99 ratios at most 1.10 median and 1.25 individually, and at most 10% within-variant variation. A noisy or incomplete result cannot grant support. These short diagnostics never grant production performance acceptance; the full original-baseline campaign remains necessary.

The original Crucible mixes PID and wall-clock time into its PRNG seed. Paired runs therefore share the operation distribution but **do not replay identical input sequences**. Earlier tail variation cannot be assigned to scheduling or input variation from the retained measurements. Both first diagnostics preserve that fixture. If noise prevents a conclusion, use a separate sparse phase/lock/queue diagnostic or a prospectively declared deterministic-seed fixture applied identically to every compared source; do not repeat short trials seeking a pass.

The [control source receipt](control-source.json) records the exact Git archive. The [capture manifest](control-capture.json) binds compiler identity, default features, profiles, source hashes, benchmark fixtures, and preserved executables. The Rust compiler and benchmark fixture match the earlier fine-histogram reference. All captures use independent build directories; no compilation, tests, solvers, or profilers may overlap timing.

`runner.py` validates the declared candidate patch by reversing it back to the complete control source map, checks identical compiler/configuration/fixtures and retained validation evidence, and preserves every run. Its receipts depend on the reviewed runner and trusted workflow; they are not independent execution attestations.

Both independent diagnostics completed, with all 12 runs passing correctness and resource checks. Captured sources and main's unchanged native source were verified afterward; no benchmark processes or disposable arenas remain. See the [postflight receipt](source-integrity-after-diagnostics.json).

| Isolated candidate | Throughput ratios, pairs 1/2/3 | Median throughput change | Conservative median p99 upper ratio | Decision |
| --- | --- | --- | --- | --- |
| [WAL reuse](wal-reuse/result.json) | 0.99448 / 1.02005 / 1.01526 | +1.53% | 0.98346 | Improvement criterion not met: median below 3%, one slower pair |
| [Removal window](remove-window/result.json) | 1.06520 / 1.06795 / 1.02641 | +6.52% | 0.94117 | Inconclusive: candidate p99 relative-range upper bound 11.50% exceeds 10% |

WAL reuse passed the existing nonregression and noise margins, but that is distinct from the stricter declared improvement criterion. Removal-window throughput and every p99 ratio met their thresholds; candidate p99 intervals were 113664–114687, 115712–116735 and 125952–126975 ns, so the fixed noise rule still prevents support. Neither result grants performance acceptance, and neither candidate was adopted after these short screens. No combined candidate or repeated 30-second trial was run.

Mechanism evidence is independent of timing: the WAL candidate's warmed outer serializer made zero allocations/reallocations across 100 byte-equivalent frames, versus 200 allocations and 800 reallocations originally; it bounds additional retained backing at 72 KiB per committer. The removal candidate's separate instrumented probe reduced the normal last-posting search count from two to one and exercised both fallback paths. These measurements establish removed work, not an accepted overall speedup.

## Prospectively declared controlled longer study

The [seeded full-study plan](seeded-full-plan.json) was declared after preserving both short results. It compares the original `a382ce3` reference directly with the isolated removal-window candidate using the same optional-seed fixture, three 120-second churn pairs and one 240-second stability pair, plus the unchanged Extended Crucible and synchronous/asynchronous WAL workloads. Pair seeds are `2026092301`, `2026092302`, `2026092303`; the sustained pair uses `2026092399`. Every existing correctness, resource, ratio and variability threshold remains unchanged. The fixed per-worker input prefixes remove PID/time seed differences; scheduling, operation counts and event timestamps remain concurrent.

The [fixture receipt](seed-fixture/fixture.json) and [validation](seed-fixture/validation.json) preserve six helper tests and executable checks of seed zero, maximum u64, absent seed, and invalid-seed rejection. The exact fixture patch reverses back to each preserved parent, with unchanged production hashes: [original reference](baseline-seeded-source.json), [candidate](remove-window-seeded-source.json), and [untimed 9676 reference](control-seeded-source.json). The latter is archived without another build or timing campaign.

The reusable comparator now injects the declared seed explicitly and requires one exact matching seed/algorithm marker in each output. Its 26 tests include missing, duplicated, malformed and mismatched markers, seed zero, pair identity, and ambient-seed replacement. The [previous comparator and tests](preseed-runner/provenance.json) remain preserved for historical receipt bindings. Main-checkout application of the candidate was provisional for fresh verification; the completed study below still does not grant performance approval.


## Completed controlled study

The [full campaign](seeded-full-campaign/campaign.json) completed all 26 runs. Every run passed its execution, correctness and resource checks, including 360,704,601 total churn operations. **The overall performance gate remains inconclusive**, because the original reference's 120-second p99 variability exceeded the unchanged 10% ceiling. The earlier measured throughput regression was absent in these controlled pairs; this is not an accepted overall speedup or a relaxation of the gate.

| Workload | Median paired throughput change | Tail result | Gate result |
| --- | ---: | --- | --- |
| Extended Crucible, 64 families / 8 workers | −1.78% | p99 +1.97% | Pass |
| Extended Crucible, 256 families / 16 workers | −0.17% | p99 +0.33% | Pass |
| Synchronous WAL, 10,000 updates | −0.75% | Not measured | Pass |
| Asynchronous WAL, 10,000 updates | −0.47% | Not measured | Pass |
| Original Crucible, three 120-second pairs | +1.23% | Reference-only variability exceeds limit | Inconclusive noise |
| Original Crucible, one 240-second pair | Timing not evaluated | Stability and resources pass | Pass for stability only |

For the 120-second pairs, throughput ratios were 1.00062, 1.02992 and 1.01230, with reference/candidate throughput variation of 3.08%/1.31%. Candidate p99 intervals were 131.072–133.119, 131.072–133.119 and 135.168–137.215 microseconds; paired reference intervals were 135.168–137.215, 237.568–239.615 and 421.888–425.983 microseconds. Every observed candidate upper endpoint is below its paired reference lower endpoint. That descriptive result does **not** override the declared variability gate: conservative reference p99 variation was 122.41%, versus 4.69% for the candidate. No cause for reference variability is established by these measurements, and no further trial was run.

The 240-second candidate completed 71,680,025 operations at 298,656.13 operations/second, with retained throughput 0.9792 and a 35,948,056-byte arena high-water mark. The reference retained throughput was 0.9808. Both runs completed their final index, allocation and reclamation checks. This single pair supports finite-run stability and resource bounds, not timing acceptance or indefinite-runtime guarantees.

Fresh validation preceding timing passed [441 native tests, with two ignored](native-validation/receipt.json), and [all 19 composed verification checks](formal-validation/report.json). The proofs remain scoped to the stated kernels, conditional driver contracts and bounded models; whole-engine verification is still open. Removal-window safety also has native regression, upper-lane omission and fallback checks; the conditional driver proof alone does not prove native skiplist correctness. The WAL-reuse and rejected lock/layout candidates were not included.

The [execution receipt](seeded-full-execution.json), [analytical summary](seeded-full-summary.json) and [postflight receipt](seeded-full-postflight.json) bind the declared plan, exact seeds, compiler, captured source, binaries and outputs. Postflight verified all 233 formal source files and 118 current native source files, both immutable captures, and all 26 output/binary hashes. No benchmark processes or disposable arenas remain. Historical results and thresholds are unchanged; there is no performance approval from this campaign.

## Engineering acceptance

On 2026-09-23, after reviewing the measured throughput recovery, observed p99
consistency and the automatic gate's limitation, the project maintainer stated:

> I choose to accept the implementation as an engineering improvement. you may commit and push

The removal-window implementation and optional benchmark seed support are
therefore accepted for the project. This is an explicit engineering decision;
the campaign's `passed: false` and `inconclusive_noise` results remain unchanged.
The decision does not claim statistical significance from three pairs, identify
which of the preceding changes caused the observed p99 consistency, or establish
native skiplist refinement or whole-engine verification. Rejected experiments
remain excluded from the production implementation.
