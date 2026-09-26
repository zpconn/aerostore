# Next experiment after complete maintenance jobs

The complete-sweep harness is a baseline for experiments, not evidence that the current index or ownership design is optimal. Preserve the default batch control, the new whole-job accounting, the serial oracle and existing component guardrails. Do not promote an implementation from a short passing rate or from the number of committed batches.

## First: explain maintenance progress failures

The short higher-load sweep trials still expose retry exhaustion in maintenance and PostgreSQL queue pressure. The completed review supplies the exact cells; failed histories have no serial correctness verdict. These runs used accelerated timers, sixteen logical families and shared resources, so they locate failure cases rather than establish capacity ceilings or architecture rankings.

Reproduce a selected failing job with precise retry attribution. Current native counters identify stages such as `global_due_lookup`, `global_expiry_lookup`, row reads and commit, but `SerializationFailure` does not distinguish index stamp changes, historical lookup rejection, concrete row validation and predicate revalidation. Successful-query timing also omits work spent in failed queries. Failed workers may not deliver their final aggregate counters. Preserve bounded per-attempt diagnostics on the error path, and measure their overhead against the same executable with diagnostics disabled before using them for a performance conclusion.

Use the existing deterministic contention diagnostics to anchor the meaning of each cause. Extend them only for newly observed paths. Keep required true conflicts as controls: an eligible insertion must invalidate an earlier empty predicate observation where required by the transaction order. A reduction in retry counts is not acceptable if it weakens query completeness.

## Then: change one measured cause

The native benchmark expiry index currently includes active record types that PostgreSQL's partial expiry index excludes. A controlled benchmark schema variant with equivalent eligibility is a small first candidate. Hold sweep mode, seed population, batch sizes, arrival corpus, routing, worker count and acknowledgment contract fixed. Confirm identical query results in complete histories before interpreting fewer candidates or retries as an improvement. Global due-query failures are a separate mechanism; an expiry-only improvement must not be generalized to them.

Repeated complete queries inspect eligible rows again for each batch. At larger populations this can approach quadratic row visits over a sweep. Measure candidates, returned rows, transport bytes and failed-attempt work. Limited queries, pagination, versioned indexes, finer predicate tracking and owner-side transaction commands each change a different cost or semantic boundary; choose one only after the measurements identify the bottleneck. Verify the changed stable contract with the smallest relevant proof/refinement obligations and mutation controls.

PostgreSQL already prepares workload statements once per worker connection and reuses them through retries. Its current plan audit runs separate literal examples, not the actual prepared handles. Capture plans after representative repeated execution before drawing a tuned-baseline conclusion. Preserve custom/generic-plan settings and execute counts with results; do not assume that prepared statement reuse implies a particular plan.

## Scale and representativeness remain separate obligations

Parameterize provenance counts/eligibility and continuing creation/retirement, retaining distinct eligible-fork updates per message. Seven fixed views and at most three seeded expiry cohorts are not a realistic lifetime population. Daily flight volume does not determine active flights or incoming message rate. Use declared sensitivity ranges until further calibration is available.

Before 100/200/300-worker trials, review bounded execution admission and internal registration headroom. The 256-slot ProcArray is shared with temporary index-scan registrations; the service defaults to 128 sessions. Connected input workers are not the same as simultaneous transactions. Do not raise CLI limits without simultaneous begin-plus-scan, exhaustion, cancellation and permit-release tests, PostgreSQL connection headroom and complete-query transport sizing. Include admission waiting in p99 and enforce recorded host resource budgets. Choose an efficient worker count for each engine; AeroStore need not use PostgreSQL's historical count to achieve the target.

Run controlled, interleaved full-history companions and lighter sustained measurements after functional checks pass. Require useful foreground completions, eligible fork effects, maintenance deadlines, retries, queueing-inclusive p99 and retention over multiple lifecycle cycles. Establish a repeated PostgreSQL saturation bracket and an explicit matched crash/durability contract before claiming a tenfold capacity advantage. Keep production worker-death survivor continuity and later physical two-host MMHF trials as release requirements; this workload milestone does not discharge them.
