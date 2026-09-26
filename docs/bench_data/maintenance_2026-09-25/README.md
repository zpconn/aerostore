# Complete maintenance sweeps — 2026-09-25

The calibrated Crucible now completes scheduled projection and housekeeping jobs through configurable batch transactions. Each job retains its original cutoff and ends only after a committed complete query observes no eligible work. This follows the architect's recollection of batched maintenance commits; exact historical sizes and completion guarantees remain unconfirmed. The [maintenance guide](../../hyperfeed_maintenance.md) defines the simulation contract.

This changes the benchmark and qualification tools. Production engine code and the original Extended Crucible are unchanged. It removes a workload simplification; it does not establish a speedup, sustainable capacity, production worker-death availability or physical MMHF performance.

## Functional checks

The [campaign review](campaign-review.json.gz) independently reconciles **42/42 passing short trials**: direct AeroStore, the Unix-socket service and PostgreSQL; identity and temporary signature-affinity routing; three seeds; full histories and exact metrics companions. Six additional full-history cells within that total exercise projection/housekeeping batch sizes 1/16 and 16/64, alongside the default 4/32. All compared sweep populations and foreground corpora match within each seed. A separate local TCP sweep run also passes. Loopback is transport evidence, not a two-host measurement.

All successful full-history functional trials have valid serial witnesses. Metrics-only runs remain unverified even when their full-history companions match. Independent review checks every recorded batch, fixed cutoff, terminal query, logical job count and transaction count. Different valid transaction orders may produce different batch counts.

The development process suite passed **19/19 tests** against the identical final executable: 11 existing integration tests, two calibrated pause tests, two affinity tests and four new maintenance tests. The new cases check complete sweeps, the preserved one-batch control, exact full/metrics companions, a cap failure after committed work, and a paused projection worker that retains both deadlines and drains after admission. A normal new sweep test completes 52 logical jobs through 65 transactions; committed batches do not inflate message throughput. Deliberate cap failure is retained as expected failure evidence, not counted as a successful truncated sweep.

Final unit checks passed **51 model, seven measurement, 51 qualification and 12 remote-helper tests**. The [implementation review](implementation-review.json.gz) found no blocking issue and verifies exact byte preservation of production core and original Extended Crucible sources relative to the preceding commit.

## Two real five-minute deadlines

The [cadence review](cadence-review.json) checks separate 601-second direct AeroStore and PostgreSQL runs with identity routing as an ordering control. Each completes **19,232 useful foreground messages and four full maintenance jobs through 19,256 transactions**, with a valid serial witness. Initial rows, foreground inputs and observed final rows match across engines.

At both 300 and 600 seconds, projection processes 28 events through seven positive batches and one terminal transaction. Housekeeping processes 66 then 65 expired records, each through three positive batches and one terminal transaction. Foreground input continues during maintenance. Independent review reconstructs cutoffs, raw receipts, whole-job latency and witness real-time ordering. These are two useful deadlines under a fixed synthetic population, not a turnover or memory-plateau test. The shared-host runs do not establish comparative speed or capacity.

## Higher-load progress limits

The separate [stress review](sensitivity-review.json.gz) retains all twelve full-history attempts: one seed, four foreground workers, sixteen families, five seconds, 1/2-second maintenance intervals and a 1,000-job per-worker backlog limit. The affinity TTL is an explicit experimental 600 ms.

| Routing | Offered foreground messages/sec | Direct AeroStore | Unix service | PostgreSQL |
| --- | ---: | --- | --- | --- |
| Identity | 512 | Valid | Housekeeping retry limit | Valid |
| Identity | 2,048 | Projection retry limit | Housekeeping retry limit | Backlog limit |
| Signature affinity | 512 | Valid | Housekeeping retry limit | Valid |
| Signature affinity | 2,048 | Projection retry limit | Projection retry limit | Backlog limit |

Four attempts drain with valid serial histories; eight stop at configured progress bounds. Retry-limit errors occur after 128 conflict retries. Failed traces retain received committed work and pending jobs, but do not have a complete serial-history verdict. In the native affinity run at 2,048/sec, projection committed three batches processing twelve events, then exhausted retries on its fourth batch. Those effects remain recorded, and the unfinished job is not counted complete. These failures do not demonstrate an invalid committed history, and they must not disappear from the comparison.

The trials shared a host with other validation and used accelerated maintenance and full-history instrumentation. They establish neither a controlled engine ranking nor a PostgreSQL saturation bracket. Differences from the previous batch-only checkpoint also change the workload. Current stage counters do not prove which internal conflict mechanism caused a failure.

## Verification guardrails

The [component pilot](guardrails/README.md) passed **71/71 checks** on the final source, including Lean extraction and negative controls, Verus adapters, finite TLA+ models, native mutation controls, core regressions and all three deterministic Extended Crucible feature configurations. Its 525 proof/test inputs remained unchanged. Captured Lean source and translation bytes match all 25 declared mutation/source hashes.

P0 contract coverage remains complete; full P1, whole-engine verification and architecture promotion remain false. These are the existing component obligations, not a new formal proof of the maintenance scheduler. The explicitly reviewed boundary refresh is local change control, not independent acceptance against a Git baseline.

## Accounting, seed and evidence limits

Primary completion counts and latency are per foreground message or complete timer job. Reports separately count every committed transaction, including terminal empty probes. Whole-job p99 includes queueing, all batch attempts, retries and receipt delivery. The denominator remains admission through worker shutdown and confirmed drain.

Sweep mode seeds at most three finite expiry cohorts to provide useful housekeeping at successive early deadlines. The old batch control preserves its original ages. Comparing those modes therefore changes both job execution and the seed; it is not an isolated implementation-cost comparison. Batch-size comparisons within sweep mode use identical seeds. Neither fixture models continuing population turnover.

The final [build receipt](build-provenance.json.gz) binds executable SHA-256 `c2f155c7a6f3cc9ad53730eceaa2797dd29873e6a23cdee2285f2273459afc28` to the frozen 484-file source snapshot `1307af5f3239bdcbd652ef79df57ac3a91614d10c55004d9845db18b0ad3c8cd`. The process regressions ran before the explicitly reviewed boundary refresh, against the same executable; the only source-snapshot difference is that boundary lock. Both source bundles and execution receipts are retained. This is explicit evidence reuse, not a claim that the pre-refresh tests ran on the later source fingerprint.

The [artifact manifest](artifact-manifest.json) records original and compressed hashes. Executables, mappings, WAL, private worker configurations and PostgreSQL data are omitted. Failed attempts and deliberate negative controls remain archived. The earlier calibration and affinity archives are unchanged. The [cleanup receipt](postgres-cleanup.json) confirms the exact owned PostgreSQL process had no other clients and was stopped, retaining its data directory.

The [next experiment review](next-milestone-review.md) prioritizes precise maintenance retry attribution and a controlled comparison of expiry-index eligibility before choosing a production index change. Population turnover, fork distributions, reviewed 100/200/300-worker admission, matched durability, survivor continuity and physical MMHF trials remain required for the project's ≥10× useful-throughput goal.
