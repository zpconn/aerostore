# HyperFeed cadence and ordering validation, 2026-09-25

The new `--workload calibrated` profile implements the architect's guidance that
messages for one flight normally arrive in order and global projection and
housekeeping each normally run every five to ten minutes. It preserves the
existing fleet/lifecycle/conflict workloads as stress coverage. This checkpoint
validates the scheduling, model, accounting and transport behavior; it does not
establish sustainable capacity, a 10× improvement, or a production replacement.

## Implementation and limits

Foreground input has a fixed offered rate. One flight stays on one foreground
worker through retries, while different flights can execute concurrently.
Projection and housekeeping each have an additional worker and an independent
wall-clock schedule. Admitted timer jobs retain their original deadlines through
queueing and must drain; caps and excess backlog fail rather than skip work.

The simulation's strict processing order is stronger than the supplied normal
input-ordering observation. Its fixed seeded population, quiet quarter, one-plan /
15-position mix, three sources and age distribution are explicit assumptions.
Each maintenance tick runs one complete global query followed by a bounded batch:
at most four projection events or 32 housekeeping records. Full maintenance sweeps
and flight turnover remain missing from this profile.

Model review found that second-resolution event timestamps would incorrectly
classify consecutive same-second foreground messages as stale. This profile now
uses nanosecond event timestamps and deadlines, retaining second-based scheduled
flight identities. Projection converts elapsed time back to physical seconds.
Tests cover equivalent physical results, invalid scaling/overflow without mutation,
and unchanged legacy serialized messages. Production engine code is unchanged.

## Completed validation

| Check | Result | Evidence |
| --- | --- | --- |
| Accelerated full histories: three engines × three seeds | 9/9; 326 messages each | [Campaign](accelerated-full/campaign.json.gz) |
| Matching metrics runs | 9/9; exact input companions; histories unverified | [Campaign](accelerated-metrics/campaign.json.gz) |
| Native AeroStore, 601 seconds | 19,232 foreground + 2 projection + 2 housekeeping; Valid history | [Campaign](cadence-native/campaign.json.gz) |
| PostgreSQL, 601 seconds | Same counts; Valid history | [Campaign](cadence-postgres/campaign.json.gz) |
| TCP loopback | 196 messages; Valid history; confirmed drain | [Report](loopback/client-report.json.gz) |
| Preserved conflict/fleet scenarios, both engines | 12/12 Valid histories | [Report](preserved-stress/report.json.gz) |
| Real process integration | 11/11 | [Log](integration-final.log) |
| Paused maintenance worker controls | 2/2 | [Log](pause-tests.log) |
| Calibrated / existing model / measurement tests | 27 / 45 / 6 passed | [Log](model-and-measurement-tests.log) |
| Qualification / remote-helper unit tests | 37 / 8 passed | [Qualification](qualification-tests.log), [remote](remote-tests.log) |
| Existing component verification pilot | 71/71 | [Guardrails](guardrails/README.md) |

The calibrated 27-test target includes 15 imported existing model tests. The two
pause controls exercise successful catch-up after admission and an expected
backlog failure with uncompleted offered jobs preserved. They do not test or
change production worker-death recovery.

The accelerated matrix uses four foreground workers, 16 seeded families, a 64/s
foreground rate, five-second admission, and 1/2-second projection/housekeeping
intervals. The real-cadence runs use the same worker/population settings, 32/s,
601-second admission and 300-second intervals for both maintenance kinds. Their
jobs are due at 300 and 600 seconds. Default 300/600-second interval arithmetic
has model coverage; this archive contains no separate 1201-second default run.

Every foreground message in the new-profile campaigns did useful work, and every
admitted maintenance job had positive effects. Both real-cadence runs preserved
16 live families, with all four maintenance jobs overlapping foreground execution
intervals. That overlap includes retries/backoff; it does not prove individual
queries overlapped. Each projection tick handled four of 28 due events; the two
housekeeping ticks removed 32 of 196 and 32 of 164 eligible records. These observed
counts confirm the bounded-batch limitation.

The [independent cadence audit](cadence-review.json) reconciles all 38,472 recorded
transactions with the expected corpus, timer deadlines, per-flight order, class
counts, outcomes, latency arithmetic, worker activity and continuous admission-to-
drain timing. It checks witness coverage and real-time precedence and inspects the
oracle's Valid result; it does not independently rerun the semantic oracle.

## Evidence scope and next work

Runs overlapped other validation on this host. Their timing and retention samples
are diagnostics, not controlled engine rankings, saturation measurements or a
memory plateau. Metrics companions do not prove their unrecorded histories. The
gate deliberately leaves capacity, architecture promotion and actual HyperFeed
replacement flags false even for the long runs.

The harness retains asynchronous WAL acknowledgment and explicit normal-drain contracts.
PostgreSQL runs natively on this WSL host over a private Unix socket, with fsync
enabled. Matched crash-loss/recovery contracts and enforced equal resource budgets
remain qualification work. TCP loopback establishes local protocol behavior;
physical MMHF hosts have not been measured. Production direct access still has
the previously documented worker-failure availability gap.

The next workload step is to make each scheduled maintenance job finish its full
sweep through bounded transactions, then add calibrated lifecycle turnover and
population/message distributions. Sustained, isolated, matched-contract trials
can then guide conflict tracking, indexing and ownership changes. Existing stress
tests and verification checks remain guardrails throughout. See the
[profile runbook](../../hyperfeed_calibrated.md) and
[qualification plan](../../hyperfeed_qualification.md).

## Reproduction and preservation

[Build provenance](build-provenance.json.gz) records the pinned Rust 1.93.1 build,
exact Cargo artifact, private binary hash and identical before/after source
fingerprints. The [source bundle](source.tar.gz) preserves all 481 declared build
inputs; the guardrails archive supplies its additional 41 proof inputs. All
campaigns use the same source fingerprint and private binary. Working-tree paths
in original receipts remain historical paths, not portable archive links.

The [preservation audit](preservation-review.json) verifies the source bundle,
binary, 40 retained runtime/original Extended Crucible paths and all 293 historical
architecture-archive files. The previous boundary rejection is retained alongside
the reviewed 373-path local boundary. It remains `local_bootstrap_only`, without
independent baseline anchoring, full P1 completion or whole-engine verification.
The existing P0 contract-coverage audit passes (32 modules, 425 public APIs and
31 declared lock edges); this is source-fresh contract coverage, not semantic
verification or inferred deadlock freedom.

[Final review](final-review.json) summarizes the reconciled evidence and limits.
The [artifact manifest](artifact-manifest.json) maps each original path to its
archive copy, with both original and compressed hashes and sizes. The guardrails
subdirectory has its own member manifest. Gzip histories/logs preserve original
bytes, including deliberate negative controls. Executables, mappings, WAL,
private worker configurations, PostgreSQL data and earlier development attempts
are omitted. Progress files are in-flight snapshots; completed campaign and run
reports carry the final status. [SHA256SUMS](SHA256SUMS) covers the complete
published checkpoint.

After all database tests, the identity-checked disposable PostgreSQL server was
[stopped](postgres-cleanup.json), preserving its data. No unrelated service was
stopped or removed.
