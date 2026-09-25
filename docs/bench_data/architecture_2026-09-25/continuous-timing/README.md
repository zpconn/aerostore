# Continuous drain timing correction

The corrected harness measures one uninterrupted client-clock interval from admission through worker shutdown and confirmed drain. All three local paths pass the repeated 100 offered messages/s check under the declared short-run policy. Higher-rate diagnostics still reproduce the native and interactive-service concurrency penalties. They do not qualify a 1,000/s capacity, select an architecture, or demonstrate a 10× advantage.

These measurements use source SHA-256 `d817ba814d857356f29c70d1f5dce340e380ce453601d028b69df939df6a5c4a` and binary SHA-256 `346d9393338c7ebdbce5c8975f05a06057176d0a53ef9d4de5739dcdc4db20a3` ([build provenance](../continuous-build-provenance.json), [exact source bundle](../continuous-source.tar.gz)). Source and binary remain stable across all four campaigns. The [raw archive](../timing-correction-campaigns/README.md) preserves every original report, receipt, and failed policy outcome. The prior stress and fleet campaign bytes remain separate; their old capacity selections are withdrawn, while their latency, retries, outcomes, and recorded histories remain evidence.

## The defect and its regression

The previous denominator added workload time to a separately measured drain time. It excluded receipt-file flushing and worker shutdown between those intervals, allowing asynchronous WAL to advance during uncounted time. The corrected report records ordered `admission_started_ns`, `workload_completed_ns`, `workers_stopped_ns`, and `drain_confirmed_ns` on one client monotonic clock. Total elapsed time and throughput come directly from the first and last timestamps. The remote path also uses the client clock through receipt of the confirmed final frame; it does not compare clocks across hosts.

The gate checks timestamp order, both elapsed scalars, and completed messages divided by continuous elapsed time. An older report can retain a valid history while failing capacity qualification and being ineligible to establish a saturation upper bound. It is not silently upgraded to the new timing contract.

An [independent arithmetic audit](independent-clock-audit.json) reconciles timestamp order, continuous duration, and throughput in all 42 corrected campaign reports. A separate native `--mode all --arrival-rate 100` [functional smoke receipt](all-mode-functional-receipt.json) and [log](all-mode-functional.log) record six Valid cases: five bounded scenarios and the fixed-arrival stream. This also covers the bounded-scenario epoch branch. That smoke run overlapped the proof pilot and supplies functional evidence only; its latency/throughput is not used for comparison.

The [negative control](continuous-negative-control.log) runs the new numerical regression against the earlier `f37b288b…` binary with an injected 400ms coordinator wait-for-worker delay. It fails for the actual rate mismatch: the old report claims `0.9547182189656775` messages/s when the conservative bound is `0.7404264264319363`. This establishes the omitted-time defect independently of missing metadata. The [corrected binary passes all seven integration tests](continuous-integration.log), in 8.961 seconds; [explicit execution receipts](continuous-integration-executions.json) associate the expected old failure and new pass with their respective binaries. The [qualification unit suite](continuous-timing-qualification-tests.log) passes 26 tests. [Log hashes](test-evidence-hashes.json) are retained; the generated interposition C source is embedded in [the regression source](../../../../scripts/test_contention_integration.py) and the source bundle. No generated shared object is archived here.

## Repeated lower-rate checks

The [full review](full-review.json) and [metrics review](metrics-review.json) each retain eighteen cells: direct native, Unix service, and PostgreSQL; one and four workers; three seeds (`20260924`–`20260926`); three seconds at 100 offered messages/s. Each cell processes the complete 300-message fleet corpus, exceeding the 256-input full-cycle requirement for sixteen configured identities.

All eighteen full histories are Valid. All eighteen metrics executions complete and have matching full-history companions; their own histories remain explicitly unverified. Every cell passes continuous timing, population, background-work, and declared performance checks. Outcomes match exactly across engines, worker counts, and evidence modes for each seed. All have zero retries, missing/deferred inputs, and stale observations. Initial populations are 12–13 live physical families and final populations 11–13; these are snapshots, not measured concurrent minima.

| Engine / workers | Full p99, three seeds | Metrics p99, three seeds | Metrics continuous completion rate |
| --- | --- | --- | --- |
| Direct native / 1 | 2.79–3.08ms | 2.75–3.01ms | 98.44–98.73/s |
| Direct native / 4 | 2.86–3.01ms | 2.81–3.58ms | 97.63–98.13/s |
| PostgreSQL / 1 | 1.71–2.12ms | 1.77–1.87ms | 99.67–99.71/s |
| PostgreSQL / 4 | 1.99–2.86ms | 2.07–3.01ms | 99.18–99.18/s |
| Interactive Unix service / 1 | 4.55–5.45ms | 4.56–5.04ms | 98.44–99.04/s |
| Interactive Unix service / 4 | 4.63–5.32ms | 4.51–4.76ms | 97.88–98.13/s |

All three engines qualify the tested **100 offered messages/s** operating point under the 50ms p99 and 95% continuous completion-rate policy, with one worker selected by the tie-break rule. Across both modes the actual completion rates are 97.43–99.71/s, including shutdown and drain. This is a qualified offered-rate lower point under that short-run policy, not proof of indefinite 100/s completion capacity. No higher repeated rate or saturation ceiling is established on this corrected build.

## Single-seed higher-rate diagnostics

The [high full review](high-full-review.json) and [high metrics review](high-metrics-review.json) each retain three cells at 1,000 offered messages/s, four workers, five seconds, and seed `20260925`. All six executions complete their 5,000-message corpora with valid continuous timing. The three full histories are Valid; the three metrics histories remain unverified. A campaign-level `passed=true` means its executions pass correctness/evidence checks, **not** that every cell passes the performance policy.

| Engine | Full p99 | Metrics p99 | Full / metrics completion rate | Full / metrics retries | Full / metrics missing+deferred |
| --- | --- | --- | --- | --- | --- |
| Direct native | 1,858.11ms | 2,003.95ms | 744.59 / 791.19/s | 18,265 / 16,072 | 29.64% / 32.36% |
| Interactive Unix service | 11,955.38ms | 12,899.77ms | 292.45 / 276.13/s | 30,651 / 31,140 | 10.04% / 10.66% |
| PostgreSQL | 6.30ms | 6.82ms | 995.16 / 995.15/s | 1,172 / 843 | 0% / 0% |

Native and service cells fail the latency and drained-rate requirements in both modes; native also exceeds the missing/deferred threshold. PostgreSQL meets the single-run policy, but one seed cannot establish a repeated capacity lower bound or ceiling. Different useful effects also preclude an equal-work speed ratio: full native produces 2,539 outputs, the service 5,809, and PostgreSQL 8,959; the corresponding metrics totals are 2,219, 5,588, and 9,008. Permitted missing/deferred and stale outcomes are not evidence of lost committed rows.

The four corrected campaigns provide **21 recorded serial witnesses**, plus 21 explicitly unverified metrics executions. All failures from the earlier campaigns remain retained; this smaller rerun neither erases their progress failures nor reproduces every earlier rate/worker/seed combination.

## What follows from this checkpoint

The timing defect is fixed and directly regression-tested. The supported repeated offered-rate point is 100/s, while the old 1,000/s capacity selections remain withdrawn. The high-rate diagnostic confirms that expensive recording was not the sole cause of the native concurrency problem.

The next controlled comparison should align the native expiry-index schema with PostgreSQL's partial index, then measure exact abort causes and historical indexed visibility. Genuine global-query conflicts remain possible, and the schema control cannot explain every global-due cost. The interactive prototype also combines database ownership with per-operation RPC costs. Background cadence, same-identity ordering, retention, and other domain assumptions need HyperFeed calibration; worker availability, crash recovery, resource equivalence, and physical MMHF performance remain separate obligations. No architecture winner or 10× claim is established.
