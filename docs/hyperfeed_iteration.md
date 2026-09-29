# Fast HyperFeed performance iteration

Use short paired screens to choose experiments. Run sustained qualification for
the few candidates worth promoting. A small edit should not automatically launch
another multi-hour capacity campaign or evidence upload.

| Stage | Purpose | Typical work |
| --- | --- | --- |
| Focused checks | Catch a broken change immediately | Relevant unit tests, affected implementation/proof checks, and a component benchmark when useful |
| Foreground screen | Look for a useful performance signal | Four 30-second admissions: baseline/candidate for one seed, candidate/baseline for another |
| Maintenance screen | Reject regressions under overlapping background work | Four 40-second admissions with five-second projection and housekeeping timers |
| Promotion | Establish a sustained result | Full-history guardrails and repeated 905-second runs with the original 300-second cadence, followed by evidence closeout |

The short lanes have two minutes and two minutes forty seconds of admission, respectively;
initialization, drain, structural audits, resource checks and result writing add
wall time. Expect roughly five to ten minutes for an ordinary paired screen,
after compilation. Measure that cost on the actual machine. The first build of
the shared development cache is slower; subsequent tweaks reuse dependencies.
Short screens do not establish long-term retention or production capacity.

The first local validation on September 29, 2026 measured:

| Operation | Observed wall time |
| --- | ---: |
| Four-cell foreground control | 3m41s |
| Four-cell maintenance control | 4m20s |
| First build in the shared development namespace | 24.3s |
| Unchanged-source recapture using the warm cache | 1.2s |

Build times include the resource controller's setup and teardown. The warm
recapture did not change Rust source; an implementation edit needs recompilation.
These are observed local costs, not runtime guarantees. Both screens used
identical executables within each comparison and therefore found no optimization.
The foreground control showed about a 10% apparent p99 improvement from noise;
the maintenance pairs pointed in opposite directions and were inconclusive.
The current selection policy requires consistent paired results and a 20% p99
improvement of at least 1 ms, or an improvement in completion/requirements.
That threshold is not a statistical guarantee. Small gains need additional
measurements, ideally a focused component benchmark before a longer trial.

All eight workload cells passed their screen requirements. Each maintenance
cell completed seven projection and seven housekeeping jobs on time, with at
least two jobs in each class doing positive work. The 112 focused Python tests
passed. See the [validation record](bench_data/hyperfeed_iteration_2026-09-29/validation.json)
for hashes, timing, resource checks and local evidence paths. The original
capacity baseline and engine implementation are unchanged.

## Commands

Create one session so all its captures, measurements and Git growth share the
same immutable 20 GiB growth budget. The runner also reserves 30 GiB on Linux
and the Windows volume hosting the WSL VHDX. Set `--host-volume` if that volume
is not `/mnt/c`; the option declares the actual volume, it does not discover it.

```sh
python3 scripts/iterate_hyperfeed.py init --output target/perf-session
python3 scripts/iterate_hyperfeed.py capture --output target/perf-session/baseline
```

Make one implementation change, then run its focused tests and capture it:

```sh
python3 scripts/iterate_hyperfeed.py capture --output target/perf-session/candidate-01
python3 scripts/iterate_hyperfeed.py screen \
  --baseline target/perf-session/baseline \
  --candidate target/perf-session/candidate-01 \
  --allow-change aerostore_core/benches/contention_crucible/service.rs \
  --rate 3584 --lane foreground \
  --output target/perf-session/candidate-01-foreground
```

List each changed implementation file with `--allow-change`. The runner refuses
changes to workload, arrival scheduling, accounting or fixture code; those need
a separate experiment with a newly measured baseline. Compiler settings and the
qualification driver must agree. Test-only changes are recorded separately.

If the signal is useful, run the maintenance lane with the same captures:

```sh
python3 scripts/iterate_hyperfeed.py screen \
  --baseline target/perf-session/baseline \
  --candidate target/perf-session/candidate-01 \
  --allow-change aerostore_core/benches/contention_crucible/service.rs \
  --rate 3584 --lane maintenance \
  --output target/perf-session/candidate-01-maintenance
```

Use a fresh output name for every attempt. An interrupt stops the current owned
processes and prevents further cells from starting; its partial evidence remains.
The runner does not resume or overwrite incomplete cells. Existing capture
directories and prior screen outputs remain usable and unchanged.

A control can use the same capture for both arguments with no `--allow-change`.
A completed control with valid resource evidence reports `control_only`, even
when ordinary run-to-run noise makes one label look faster. Historical capacity
build receipts can be imported with
`import --build-receipt PATH --output DIR` for such controls. Imported and fresh
captures have different build recipes and cannot silently be compared as a
matched treatment; capture a fresh baseline before testing new implementations.

## What a screen measures

The default profile keeps the established 1,024 identities, 16 foreground
workers plus two maintenance workers, message mix, seven forks, temporary
signature affinity, ordering, native index policies, local Unix service and
asynchronous WAL settings. It enforces the same 24-logical-CPU affinity and
36 GiB memory ceiling, with a 4 GiB available-memory reserve. Swap, resource
interruptions, generator limits and invalid evidence cannot become a performance
win. Core dumps are disabled inside the limited process tree.

Every input is counted once. Completed-message throughput excludes completions
after arrivals stop; p99 includes queueing, retries and the final drain. The
screen also checks queue trends, ordering, useful work, structural audits,
allocation/reclamation counters and maintenance deadlines. It retains the
existing metrics receipts. It deliberately omits full-operation histories and
does not claim a full-history correctness pass.

The foreground lane ends before the first natural maintenance tick. The separate
maintenance lane deliberately accelerates timers to expose overlap; it is not
a model of HyperFeed's actual five-to-ten-minute cadence. Both use the original
50 ms foreground p99 requirement, with five seconds of queue warmup. Their
policies are separate from the unchanged sustained-capacity acceptance policy.

Results live in `screen.json`, with per-cell reports and resource receipts.
Comparisons report `promising`, `neutral`, `regression` or `inconclusive` and
the actual ratios. The thresholds are engineering screening rules, not confidence
intervals. A fixed offered rate caps measured throughput: equal completions with
lower p99 indicate latency headroom, not a demonstrated capacity increase.
Try an adjacent rate for promising candidates, then qualify sustained capacity.
Small or inconsistent differences need more evidence; do not chase their rank.

## Builds, evidence and proof scope

Captures freeze source bytes, the executable, compiler/build settings and runtime
dependency hashes. Trials use the qualifier inside that frozen source tree.
One stable, separately admitted Cargo cfg namespace reuses the normal Cargo
target across edits. Only copied executables are retained evidence; mutable
compiler-cache executable paths are not published as retained artifacts. This
does not change the fresh isolated targets required by formal verification.
Do not edit source or launch another Cargo build while capturing a variant.

Source snapshots, small summaries, failures and binaries remain local throughout
exploration. No archive or upload is in the per-tweak critical path. Preserve
the evidence and follow the [disk runbook](disk-space.md) when closing a campaign;
never clean the entire target or delete a referenced artifact. Use one active
development cache instead of a full dependency build for each candidate.

Formal verification can make correctness checking more selective when a proof's
implementation connection and complete dependencies are known. Unchanged proof
subjects need no new mathematics merely because an unrelated adapter changed.
Changed subjects still need their corresponding checks. A proof about a model
does not cover an unconnected socket implementation, and a source hash alone is
not an implementation-refinement proof. Preserve the existing promotion gates;
an impact report is advisory and is not permission to reuse a stale green gate.

Each screen writes `proof-impact.json`. It compares the captures against the
declared component inputs in `verification/refinement_campaigns.json`, lists
affected components and their existing commands, identifies missing dependencies,
and reports drift from the full gate's frozen boundary. Baseline drift is kept
separate from the candidate's changes. The current full gate has no automatic
affected-only receipt cache, so this report never grants a formal pass or skips
an existing acceptance rule.
Suggested proof commands use a unique output namespace per report. They are
templates: each destination must still be absent before execution; do not rerun
one into retained evidence.

The socket service is outside the declared inputs of the current refinement
campaigns. Its separate protocol model explicitly lacks implementation refinement
and does not cover socket parsing. A frame-write optimization therefore keeps
native partial-write, timeout, interruption and lost-reply tests. A useful future
proof target is the framing helper's byte-sequence and cursor contract, connected
to the actual implementation; that could reduce repeated correctness exploration
for later changes inside that contract. It would not predict runtime speed.

The [sustained baseline](hyperfeed_sustained_capacity.md) remains the capacity
reference. Short-screen wins never replace its qualification requirements.
