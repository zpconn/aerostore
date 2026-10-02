# Expiry resumption and PostgreSQL statistics control

The resumed experiment supports narrower expiry conflict tracking, and an explicit early PostgreSQL statistics update produced a working PostgreSQL control. Both findings help establish a fair route toward the project's 10× goal. Neither is a sustainable-capacity result or a measured speedup ratio.

The [earlier expiry experiment](hyperfeed_expiry_range.md) and [original archive](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/expiry_range_2026-09-28/artifact-manifest.json) retain their completed, failed, and interrupted attempts. This report's [resumption archive](https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/docs/bench_data/expiry_resume_2026-09-28/artifact-manifest.json) keeps new attempts separate, including failures and recovery evidence. Benchmark source remained at SHA256 `137b9743581329673d38daf2ec609678045f3f67f3965ddec0237b1b7b52e117`, using the retained source-bound binaries. No production engine algorithm or default changed.

The six-cell resumption reran seed 20260928 on one boot: 185 seconds per planned cell, 32 configured flight identities, 16 workers, 256 offered foreground inputs per second, a 640-message rolling cycle, and 40-second retention. Projection and housekeeping ran every five seconds, with batch caps of four and 32 rows respectively. Expiry eligibility stayed `all-active`, due publication stayed `hashed`, and the transaction retry budget stayed **128 retries, permitting at most 129 attempts**. The five-second cadence deliberately stresses overlap; real HyperFeed background sweeps were described as occurring roughly every five to ten minutes.

The timing of separate disk cleanup relative to these earlier measurements is uncertain. Their latency values therefore remain **descriptive observations, not a clean comparative performance measurement**. Foreground p99 includes arrival queueing, retries, and retirement-control inputs. Housekeeping p99 covers the complete job, including every batch and its terminal empty transaction.

| Earlier resumption cell | Outcome | Foreground p99 | Housekeeping retries | Whole-housekeeping-job p99 |
| --- | --- | ---: | ---: | ---: |
| Ordered service, full history | Valid; history verified | 2.739 ms | 38 | 368.16 ms |
| Ordered service, metrics | Valid; exact full-history companion verified | 2.688 ms | 35 | 317.18 ms |
| Hashed service, full history | Failed after 128 retries | — | — | — |
| Hashed service, metrics | Failed after 128 retries | — | — | — |
| PostgreSQL, full history | Failed after 128 retries | — | — | — |
| PostgreSQL, metrics | Failed after 128 retries | — | — | — |

Each ordered success completed 47,288 useful business messages and 36 jobs of each background class, with recurrent positive maintenance and rolling generation reuse. Metrics-only execution uses its exact successful full-history companion; it does not independently verify a full history. Failed runs retain partial progress and errors, but provide no completed-run throughput denominator.

A separate ten-second diagnostic shifted the ordered mapping into its overflow bucket. Its full history passed correctness checks, but housekeeping accumulated **574 retries**. Across all workers, diagnostics recorded **557 expiry commit-stamp rejections**; that count is not an attribution of every housekeeping retry to a particular writer. The current one-second mapping has 4,093 interior intervals—about **68 minutes of precision**—and does not automatically follow wall-clock time. Saturation retains conservative dependencies while losing selectivity. A long-lived deployment needs an explicit window strategy with correct transitions. Ordered publication remains optional; hashed publication remains the default.

PostgreSQL's failure prompted a separate control: issue one `ANALYZE` of the owned table five seconds after admission, retaining business semantics and the frozen benchmark. The first full-history workload completed 185 seconds and passed history verification, but its external treatment audit mishandled the table OID. That audit failure and helper version remain preserved; they were not relabeled as a qualified treatment pass.

The corrected second attempt, `primary-v2`, was interrupted around 35 seconds. Its final controller and benchmark outputs are missing. Telemetry has a last parseable sample at 24.230 seconds followed by malformed trailing data. Recovery observed a different boot, absent old processes and cgroups, and a stale PostgreSQL PID file. Partial artifacts remain unchanged. Disk exhaustion and a core dump were reported; retained journal observations do not establish the full causal sequence. A partial zero-OOM sample cannot establish the interrupted run's final memory state.

The fresh `primary-v3` PostgreSQL pair subsequently **passed both workloads and their independent source, treatment, companion, memory, and disk review**. Each run completed 47,288 useful business messages, 36 projection jobs, 36 housekeeping jobs, 144 projection outputs, and 5,600 housekeeping expirations. Its metrics run has an exact successful full-history companion with the same external treatment.

| Fresh PostgreSQL control | Full history | Metrics |
| --- | ---: | ---: |
| `ANALYZE` dispatch after admission | 5.000870 s | 5.000805 s |
| Client-observed command duration | 20.85 ms | 15.75 ms |
| Manual analyze count | 1 → 2 | 1 → 2 |
| Autoanalyze count across treatment | 0 → 0 | 0 → 0 |
| Previously troublesome 35-second projection job | 12.469 ms; 1 retry | 10.278 ms; 0 retries |

That projection job processed all 24 due events and reached its terminal empty batch. Automatic analysis was first observed later, around 59 and 51 seconds respectively. These observations strongly support statistics timing as an explanation worth carrying into the benchmark design. They do **not** prove which execution plan ran during the earlier failures. This externally treated pair stays distinct from the untreated baseline; no cross-boot latency ratio is reported.

The completed six-cell campaign peaked at **2.911 GiB**; the fresh PostgreSQL pair peaked at **3.003 GiB**. Both had final evidence of zero memory-limit/OOM events and zero swap use under a 36 GiB service limit, a 4 GiB swap allowance, no CPU quota, and no `memory.high` throttling. Kernel cgroups included the harness, workers, service, and owned PostgreSQL descendants. These are campaign-wide accounting figures, not per-engine memory comparisons. Small isolated OOM tests verified containment; early probe helper source gaps and later cleanup observations are explicitly disclosed.

The new [disk-space runbook](disk-space.md) is binding. Read-only checks located the VHDX on Windows C:. The fresh pair preserved the stated reserves; post-run headroom was approximately **656.7 GiB inside Linux and 416.5 GiB on Windows**. The run plan allowed 10 GiB for remaining work and reserved 20 GiB on each filesystem. After timing finished, the archival phase received a separate 12 GiB allowance for a conservative archive-plus-Git upper bound, retaining the same 20 GiB reserves. The original planning receipt remains unchanged. No new build was needed. Source and retained binaries were checked after recovery, without moving or modifying executables. This resumption performed no additional compiler-intermediate cleanup and claims zero newly reclaimed bytes. Linux free space is distinct from space returned to Windows.

Next, make PostgreSQL's statistics lifecycle an explicit benchmark option with recorded timing and exact companion matching. Then measure matched, steady useful-message capacity across realistic populations, fork fan-out, affinity, and background cadence. Develop a long-lived expiry-window strategy alongside that work, retaining complete-query, transaction, reclamation, and recovery checks. Let measured service costs choose subsequent optimizations. This establishes the evidence needed for a 10× claim while keeping architectural experiments reversible.

Surviving-worker availability and physical two-host measurements remain separate requirements for the single-machine and multi-machine HyperFeed replacement.
