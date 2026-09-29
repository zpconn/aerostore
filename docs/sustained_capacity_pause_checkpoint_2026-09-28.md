# Sustained-capacity campaign: paused September 28, 2026

Paused at the user's request. **Wait for explicit resumption. Do not launch trials,
builds, profiles, queued stages, cleanup, or publication while paused.** All work
and artifacts remain in place. Current changes are uncommitted and unpushed;
HEAD is `512db389abd4c317b8dc716de2dd338c050e220c`.

The campaign root, abbreviated `CAP` below, is
`/home/zpconn/code/aerostore/target/sustained-capacity-20260928`.
Machine-readable state is in
[`pause-20260928/checkpoint.json`](../target/sustained-capacity-20260928/pause-20260928/checkpoint.json).
The pause audit was recorded after 2026-09-29 02:58 UTC, September 28 in Chicago.

## Safe stopping point and process confirmation

The sole remaining job, `pg-r640-s29-cap-fullguard`, ended through its existing
timeout/cleanup path at 02:54:01 UTC. No pause signal was sent and no subsequent
trial started. Its 90-second workload had finished and saved its history, final
image, and capacity accounting. The history checker did **not** finish: the
benchmark reported `disposable process group exceeded 390 seconds`.

This is a checker wall-time limit, not evidence of database saturation, an oracle
counterexample, a passing correctness guard, or a user-induced interruption.
The enclosing controller's exit status 0 means its containment and receipt work
finished; the benchmark report has `passed: false` and qualifier exit status 1.

The checkpoint audit confirms:

- All 30 launched envelopes have `owned_processes_terminated: true`.
- The last controller, coordinator, qualifier, wrapper, envelope controller, and
  PostgreSQL PIDs have exited. No campaign benchmark/build/database processes
  remain, no campaign systemd units remain loaded, and no owned cgroup contains
  processes. All delegated agents are finished with no queued work.
- PostgreSQL performed its owned fast shutdown: cleanup passed, PID file and
  socket are absent, and database files were retained.
- No artifacts were deleted, moved, stripped, or overwritten for this pause.

The authoritative audit is
[`quiescence-and-resources-v2.json`](../target/sustained-capacity-20260928/pause-20260928/quiescence-and-resources-v2.json).
The initial audit is also retained; it falsely matched its own audit shell.
Version 2 explicitly excludes the audit's ancestor processes and confirms an
empty campaign process set. `git-status.txt` and `tracked-work.patch` preserve
the working-tree status and tracked diff; new files remain in the workspace.

## Completed measurements

These are the completed 905-second measurements, with 300-second maintenance
cadence and two distinct seeds. The numbers below are provisional operational
results: final companion correctness coverage and final capacity assessment
are still pending. They are not a claim that the entire database is verified.

| Configuration | Offered inputs/s | Foreground p99, seed 20260929 / 20260930 | Outcome under 50 ms p99 requirement |
| --- | ---: | ---: | --- |
| AeroStore Unix service | 3,072 | 26.194 / 23.824 ms | Both pass operational requirements |
| AeroStore Unix service | 3,584 | 30.313 / 53.157 ms | Mixed; not a repeatably passing rate |
| AeroStore Unix service | 4,032 | 131.708 / 177.693 ms | Both fail p99 |
| PostgreSQL | 640 | 42.467 / 49.551 ms | Both pass operational requirements |
| PostgreSQL | 704 | 54.304 / 52.784 ms | Both fail p99 |

At the repeated passing points, AeroStore completed approximately 3,071.98
incoming messages per real second during admission; PostgreSQL completed
approximately 639.99. The ratio of the highest tested repeated passing rates
is **4.8× on this fixture**. This is not an exact maximum-capacity ratio or a
demonstration of 10× on production HyperFeed.

Each repeated endpoint completed three projection and three housekeeping jobs,
all positive and on time. Endpoint queues stayed bounded; in-admission
completion fractions exceeded 99%. Fork writes, transactions, retries, and
post-admission drain do not inflate incoming-message throughput. Completed
long trials passed their recorded structural/accounting/final audits; their
metrics histories do not include full serial witnesses.

The earlier PostgreSQL 1,024/s attempt exhausted the 128-retry limit after
approximately 72.7 seconds. Retain this as an operational configuration failure.
Short 120-second screens are search guidance, not sustained capacity passes.
The complete attempt inventory and raw receipts remain under `CAP/trials` and
`CAP/launches`; do not discard failing or mixed points.

Two default-binary diagnostic profiles also finished:

- `profiles/diagnostic-pg704-maintenance-s29`: 365-second workload, observer
  approximately seconds 240–360, including the 300-second maintenance tick.
  Average cgroup CPU was 6.05 cores; the foreground cohort at the maintenance
  tick reached approximately 421 ms p99.
- `profiles/diagnostic-service4032-s29`: 180-second workload, observer
  approximately seconds 5–125, with no maintenance opportunity. Average cgroup
  CPU was 13.20 cores; worker/session CPU was about 70.85% system time. Latency
  spikes also occurred without maintenance.

Analysis is preserved in `default-profile-findings.json`,
`profile-analysis-execution.json`, and each profile's `analysis-v2.json`.
System CPU and context-switch costs support investigating the service/worker
communication path, but do not yet establish that batching or an index change
will improve capacity. Scheduler statistics were disabled; zero scheduler-wait
counters cannot establish absence of scheduling delay. No optimization was
implemented from these profiles.

## Conditions and limits of the comparison

The authoritative policy is `CAP/declared-plan.json`:

- 1,024 identities: 768 active and 256 quiet; seven forks; fixed population,
  no rolling turnover, 262,144 slots, default one-hour retention.
- 16 foreground and two background processes, CPUs 0–23, identical rate/mix,
  signature-affinity dispatch with 600 ms TTL, normally ordered per identity.
- Sweep maintenance every 300 seconds, prefix selection, projection batches
  of four and housekeeping batches of 32, including terminal empty queries.
- p99 scheduled-arrival-to-receipt latency, including queueing and retries,
  at most 50 ms. At least 99% completion during admission overall and in the
  final third; bounded queue levels/trends/oldest ages after 60-second warmup.
  Background jobs must finish before the next tick; retry exhaustion and
  maintenance starvation fail the configuration.
- PostgreSQL 16.13 uses SERIALIZABLE, prepared statements, buffered writes,
  split candidate queries, initial ANALYZE plus another at five seconds,
  128 MiB shared buffers, and Unix sockets. AeroStore uses the Unix service,
  a 2 GiB arena, ordered due/expiry publication and housekeeping-only expiry
  eligibility. Native optimistic transaction semantics are checked by the
  retained tests and history guardrails.
- Both acknowledge asynchronously with WAL. PostgreSQL has fsync and full-page
  writes enabled, synchronous_commit off, and a 10-second WAL writer interval.
  Native WAL uses complete frames, 1 MiB batches, 10-second fdatasync and normal
  final drain. Equal crash-loss windows, recovery behavior, and exactly-once
  delivery are **not** established.

Limitations include the small fixed synthetic population, 16 versus historical
100–300 workers, one local host, finite initial old cohorts, no turnover, and
905-second duration shorter than the retention horizon. Circular slot aliasing
weakens retention coverage. The ordered time-index fixture has a finite static
window without rotation. The engines have different index sets; PostgreSQL's
abort logging remains enabled and can generate several GB. Seeds change some
data but not the arrival pattern. Memory trends are retained; these runs do not
prove an indefinite plateau. Physical MMHF and worker-failure availability
remain separate requirements.

## Preserved implementation and evidence

The working changes add sustained-throughput accounting, overload/maintenance
checks, a bounded larger metrics corpus, assessment tooling, tests, and docs.
The capacity build's test receipt records 19 suites and 514 test executions.
Production engine implementation and formal proof policies were not changed.

| Artifact | Location under `CAP` |
| --- | --- |
| Frozen source inventory | `source-freeze-capacity.json` |
| Recorded executable and build receipt | `builds/capacity/benchmark`, `builds/capacity/build-provenance-default.json` |
| Tests | `tests/capacity/receipt.json` |
| Per-attempt evidence and controls | `trials/<attempt>/`, `launches/<attempt>/`, `admissions/<attempt>.json` |
| Full-guard scope and prospective commands | `guardrail-coverage-amendment.json`, `guardrail-coverage-adoption.json`, `full-guard-review-and-commands.md` |
| Final assessment command template | `final-capacity-input-command-plan.md` |
| Resource reservation forecast | `remaining-resource-plan-v4.json` |
| Staged, unapplied assessor fix | `assessor-taxonomy-staging/assessor-taxonomy.patch` |
| Pause receipts and saved diff | `pause-20260928/` |

The source fingerprint is
`9afef11c62330b5ce4f67422b6a3e397389fc0600d4f57f85707c0ff95e4d184`;
the executable SHA-256 is
`03bafa5f60ae9b2eef8be2b607eb6c6dad952f1032f2d6c46d9d97b85d1d6fd9`.
The checkpoint records artifact hashes. Keep this source and executable intact;
do not rebuild inside retained evidence or overwrite used helpers.

The timed-out guard's retained evidence is:

```text
CAP/trials/pg-r640-s29-cap-fullguard/trial-control.json
CAP/trials/pg-r640-s29-cap-fullguard/postgres-cleanup.json
CAP/trials/pg-r640-s29-cap-fullguard/qualification/postgres-r640-w16-s20260929-ehwtogo8/report.json
CAP/trials/pg-r640-s29-cap-fullguard/qualification/postgres-r640-w16-s20260929-ehwtogo8/contention-crucible-NULZdO/postgres-0/
CAP/launches/pg-r640-s29-cap-fullguard/envelope/result.json
```

That evidence directory retains `history.jsonl` (about 1.65 GB), `final.json`
(about 108 MB), and `capacity-accounting.json`. Preserve the timeout verdict.

## Pending work and exact resumption entry points

**First resolve the checker limit without repeating capacity measurements.**
The earlier guard plan considered the outer qualifier timeout (90 + 1,500
seconds), but missed `isolated_case` in
`aerostore_core/benches/contention_crucible/runner.rs`: its child watchdog is
`seconds + 300`, hence 390 seconds for this guard. Native 40-second guards would
have only 340 seconds despite their substantially longer estimated replay.
The existing launch commands therefore are not ready to run blindly.

After explicit user resumption, these exact read-only commands restore context:

```bash
cd /home/zpconn/code/aerostore
export CAP=/home/zpconn/code/aerostore/target/sustained-capacity-20260928
cat docs/sustained_capacity_pause_checkpoint_2026-09-28.md
cat "$CAP/pause-20260928/checkpoint.json"
cat "$CAP/pause-20260928/quiescence-and-resources-v2.json"
git status --short
df -h . /mnt/c
du -sx -B1 target
cat /proc/meminfo
cat "$CAP/trials/pg-r640-s29-cap-fullguard/qualification/postgres-r640-w16-s20260929-ehwtogo8/report.json"
sed -n '1678,1710p' aerostore_core/benches/contention_crucible/runner.rs
cat "$CAP/full-guard-review-and-commands.md"
cat "$CAP/final-capacity-input-command-plan.md"
```

Prefer a separately bounded offline replay of the already saved full history
if practical. No supported replay-only command has yet been established, so
there is deliberately no invented replay command here. Otherwise declare and
test a bounded checker follow-up, preserving old source, binary and timeout
evidence. An outer timeout change alone cannot fix this internal watchdog.
Do not silently enlarge the 2,000,000-candidate search budget or weaken guard
coverage. Record any new build's relationship to the measured binary explicitly.

The originally planned remaining guards are PostgreSQL 704/s for 90 seconds,
and AeroStore 3,072/s and 3,584/s for 40 seconds, each with a five-second
maintenance timer and full evidence. PostgreSQL 640/s still needs an acceptable
guard. The existing executable's exact failed-run reproduction is below,
**only if deliberately requested after resolving the checker plan**, with a
fresh attempt name; it is not a command that fixes the timeout:

```bash
source target/verification-tools/environment.sh
python3 "$CAP/launch_trial_v4.py" \
  --build-receipt "$CAP/builds/capacity/build-provenance-default.json" \
  --budget-addendum "$CAP/budget-addenda/endpoint-repetitions-120g.json" \
  --engine postgres --rate 640 --seconds 90 --timer 5 --seed 20260929 \
  --evidence full --max-messages 100000 \
  --attempt pg-r640-s29-cap-fullguard-resume1 --remaining-gib 27
```

Run launchers from the host PID namespace with working user systemd access.
Each launch performs fresh admission; the remaining reservation must be
recomputed if follow-up costs or the environment change. Keep all used paths
immutable. Update the command/input plan to bind the actual acceptable guard
instead of the timed-out attempt. A new boot requires a fresh resource/host
binding; do not bypass admission failures.

After guards are accepted, use the exact input/assessment commands in
`final-capacity-input-command-plan.md`, changing only the explicitly reviewed
guard bindings and using fresh output names. `capacity_inputs_v5.py` binds
source, executable, helper, controller, envelope, and budget evidence. Check
at least three completed and two positive jobs for each maintenance class.
The assessor can exit 0 for a complete assessment containing intentional failed
rates; inspect classifications rather than interpreting that exit as all-pass.
Full guards validate their own executions, not the different long metrics
histories. Keep native 4,032/s results as supplemental unless independently
given the required same-rate coverage.

Then finish the capacity report, retry/retention interpretation, and bounded
bottleneck experiment selection. No more long baseline repeats are currently
required unless a material issue is discovered. Do not start speculative family
indexes or RPC batching merely because the profile suggests communication cost.

Archive and audited compiler-intermediate cleanup are prepared, not executed:
`capture_tool_runtime.py`, cleanup/finalize helpers v4, and
`prepare_closeout_v5.py`, `archive_campaign_v5.py`,
`verify_stored_archive_v5.py`. Refresh their closeout plan with final guards,
assessments and profile analyses. Preserve source/hash closure before applying
the staged assessor taxonomy fix (four added fixtures; 34 tests passed in
staging). That fix recognizes top-level startup errors; it does not alter the
completed sustained results. Optional retry-diagnostic feature builds are
prepared but unrun. Commit/push remains pending until work resumes and is ready.

## Disk and memory constraints

At the pause audit, allocated `target/` usage was 214,357,921,792 bytes
(199.64 GiB), growth of 72.84 GiB over the recorded campaign baseline.
Linux free space was 586,749,878,272 bytes (546.45 GiB); the fresh read-only
Windows registry/DriveInfo check confirmed the Ubuntu VHDX is on C: with
313,576,484,864 bytes (292.04 GiB) available. MemAvailable was about 45.24 GiB.
See `disk-host-refresh-pause-20260928.json` for host-volume evidence.

- Total admitted campaign growth is capped at **120 GiB**, not another 120 GiB
  from the pause point. Preserve at least **30 GiB free on both Linux and the
  actual Windows VHDX host volume**, including retained data and archive copies.
- Each whole trial uses a **36 GiB** memory envelope on **24 logical CPUs**,
  a **4 GiB** swap containment cap, and a **4 GiB** host MemAvailable emergency
  floor. Any observed trial swap/OOM/resource interruption excludes a clean
  capacity result. Existing host swap use is distinct from trial-envelope use.
- The last guard peaked around 3.75 GiB, with no swap or OOM. All trials must
  still use the envelope; do not infer future safety from that one peak.
- `remaining-resource-plan-v4.json` reserves 10 GiB for archive/Git plus
  remaining histories, checker and observer output. Its original guard
  reservations were 27/24/19/13 GiB of other remaining work; timeout follow-ups
  require a fresh forecast accounting for the retained failed history.
- Follow [the disk runbook](disk-space.md). No blanket Cargo clean, deletion of
  evidence, stripping binaries, WSL shutdown, or attached-VHDX manipulation.
  Cleanup must audit references and runtime dependencies before and after;
  report Linux space separately from Windows space. No cleanup was performed
  during this pause, so no reclaimed-space claim is made.

**Paused, with no campaign jobs running. Resume only on the user's instruction.**
