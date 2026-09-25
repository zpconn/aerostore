# HyperFeed architecture investigation, 2026-09-25

This checkpoint builds a repeatable way to investigate the goal of replacing
PostgreSQL for local HyperFeed and MMHF at at least 10× sustainable complete-message
throughput. It does not demonstrate that target, qualify a production replacement,
or select an architecture winner. MMHF means remote workers connecting to one
central database host.

The production engine and original deterministic Extended Crucible remain
unchanged from `6291200b4383538d960f3b1c73942715193f464b`. New benchmark adapters,
failure tests and a separate abstract protocol model explore the next design
decisions while retaining the component verification pilot.

## Workload correction and provenance

The investigation exposed a consequential simplification in its first lifecycle
generator: a generation completed within 16 messages, with at most one live family
in serial execution. Some global background searches therefore did no work, and
four-worker assignment placed creation on one worker. The original measurements
remain available as concentrated turnover stress evidence. They cannot establish
contention behavior for a populated HyperFeed fleet.

The separate `fleet` profile staggers lifecycles, rotates message types across
workers and initializes a populated fleet outside timing. It uses uniform traffic
and requires at least 16 identities. Serial tests retain at least 11 live families
with 16 identities and 21 with 32. Runtime reports contain initial/final population
snapshots, not a measured concurrent minimum. Qualification requires a full cycle,
creation and expiry, and positive effects from all four global background kinds.
These remain synthetic policies and workloads requiring HyperFeed calibration.

Three builds must be kept distinct:

| Evidence | Build and exact source |
| --- | --- |
| Repaired concentrated lifecycle stress | [build provenance](build-provenance.json), [source bundle](stress-source.tar.gz) |
| Populated fleet before continuous-drain timing correction | [fleet build provenance](fleet-build-provenance.json), [source bundle](fleet-source.tar.gz) |
| Continuous-drain timing correction and final source | [build provenance](continuous-build-provenance.json), [source bundle](continuous-source.tar.gz) |

Build receipts record the exact Cargo artifact, pinned compiler, dirty source
hashes before/after building, and binary SHA-256. A campaign's source and binary
must match its own receipt; a later passing build does not retroactively qualify
earlier results. The interrupted pre-reply-deadline campaign and its earlier build
are retained separately.

Final review found that the first two builds added workload elapsed time to a
separately timed drain, omitting worker shutdown in between. WAL could progress
during that omitted interval. Their p99, retry, history, outcome and retention
observations remain usable; their drained-rate capacity conclusions are withdrawn.
Raw reports preserve historical gate outputs. The corrected gate refuses reports
without a continuous client-clock interval through confirmed drain for capacity.
A regression injecting 400 ms into real coordinator worker shutdown reproduces
the old overstatement and passes after the correction.

The [corrected timing review](continuous-timing/README.md) contains 18 full and
18 metrics trials across three seeds at 100 offered messages/second, plus three
full and three metrics high-rate diagnostics. Every corrected execution completes;
the 21 full histories are Valid, while the 21 metrics histories remain unchecked.
All repeated low-rate trials pass policy, with measured continuous completion rates
of 97.43–99.71/s under the declared 95% drain-rate rule. This establishes a tested
offered-rate operating point, not a capacity ceiling or 10× result. The single-seed
high-rate native and service diagnostics fail performance policy in both modes;
their completed-execution flags do not mean performance acceptance.

## Fleet findings

The [paired fleet review](workload-and-gates/fleet-campaign-review.md) covers
36 full-history cells and 36 lighter measurement cells before the timing correction.
The full campaign has 33 Valid histories and three service backlog failures.
The measurement campaign has 31 completed executions and five failures: three
service backlog failures and two exhausted 128-retry limits, one each for direct
native and service execution. Metrics-only histories are not verified. Both
campaigns completed with stable sources and retain a failed overall gate. Their
individual capacity-policy outputs predate the timing correction and are historical only.

At 1,000 offered messages/second, the three full native one-worker runs have
5.22–6.20 ms p99 and zero retries. Four workers increase p99 to 1.46–1.89 seconds
with 17,366–17,691 retries per 5,000 messages. PostgreSQL's four-worker full runs
have 6.01–6.91 ms p99. The lighter runs reproduce the native problem; one native
four-worker seed also exhausts its retry budget. Completed native four-worker
histories permit substantially less useful work, so equal message counts are not
equivalent application work. All 100/s matrix cells completed successfully.

These are results for a demanding synthetic background cadence, not a calibrated
HyperFeed capacity comparison. Neither engine has a measured capacity ceiling;
the pre-correction gate's passing-rate bounds are also withdrawn. No 10× ratio
can be inferred. The interactive service also exposes severe per-operation
transport/concurrency costs without isolating the cost of ownership itself.

The next small control is to give the native benchmark a housekeeping-specific
expiry index matching PostgreSQL's partial index, then profile exact native
abort causes and historical indexed visibility. Native currently indexes every
active record's event time and filters irrelevant kinds after lookup. Global due
queries are already partial on both sides and remain the larger measured cost;
the expiry index difference cannot explain the entire concurrency penalty.
Calibrate global background cadence and same-identity message ordering before
using this stress mix to choose a production architecture.

All three 120-second fleet trials at 100 offered messages/second pass with
12,000 checked messages each, zero retries, identical aggregate outcomes and
12→13 live physical families. Overall p99 is 3.285 ms for direct native,
5.295 ms for the Unix service and 2.193 ms for PostgreSQL. The recorded intervals
show no overlapping messages in any of these three runs despite four configured
workers. These are serial-paced turnover/retention observations, not sustained
contention evidence. The native
allocation high-water is still increasing near the end; the run does not prove
a plateau or bounded long-run memory.

## Evidence inventory

- [Final verification guardrails](guardrails/README.md): all 71 component-pilot
  checks pass on the final frozen source, with native regressions and all three
  original Extended Crucible configurations. The earlier interrupted 37-check
  run is retained separately and is not presented as completed verification.
- [Continuous timing correction](continuous-timing/README.md): seven integration
  tests, 26 qualification tests, the reproduced negative control and corrected
  comparisons, with an independent check of all 42 timing/rate identities.
- [Corrected raw campaigns](timing-correction-campaigns/README.md): the complete
  18/18/3/3 trial receipts and histories, with verified archive manifests.
- [Corrected network timing](continuous-functional/README.md): a full-history
  loopback TCP run with client-side confirmation timing and owner cleanup checks.
- [Workload and qualification checks](workload-and-gates/README.md): independent
  serial-model checks, negative controls for the qualification gate, and process
  integration checks. Metrics-only runs explicitly lack a checked history.
- [Final fleet functional checks](fleet-functional/README.md): the frozen build's
  TCP loopback run and 64-identity native/PostgreSQL checks. These are functional
  trials, not capacity qualification or physical MMHF measurements.
- [Fleet raw campaigns](fleet-campaigns/README.md): complete full, metrics and
  retention campaigns, including failed cells, original histories and verified
  per-member archive hashes.
- [Fleet retention analysis](fleet-retention/README.md): reconciliation of all
  36,000 messages, overlap checks, useful background effects and ten-second latency
  cohorts, with separate native allocation and PostgreSQL relation gauges.
- [Service availability](service-availability/README.md): selected native client
  SIGKILL cuts, an already-running survivor process, abandoned-session cleanup,
  reply-backpressure regression, and a separate finite TLA+ protocol campaign.
- [Final-source service rerun](service-availability-final-fleet/README.md): the
  same 27-test debug target repeated against the final frozen sources, with
  separate native Unix/TCP receipts.
- [PostgreSQL and network validation](postgres-and-network/README.md): buffered
  serializable adapter tests, native-host PostgreSQL build/settings, and loopback
  orchestration evidence for the concentrated stress build.
- [Concentrated stress campaigns](stress-campaigns/README.md): both complete
  48-cell matrices, their failed cells, the interrupted first campaign, and three
  sustained runs. Compressed raw evidence has per-member hashes.
- [Concentrated stress retention](stress-retention/README.md): reproducible
  per-message and ten-second cohort analysis. These paced runs had no overlapping
  messages; they are retention observations, not contention measurements.

See the [qualification runbook](../../hyperfeed_qualification.md) for reproduction,
contracts, metrics and acceptance rules, and the [original investigation](../contention_2026-09-24/README.md)
for the production direct-access failure evidence.

## Availability and verification boundary

The required contract is that surviving workers keep processing when one worker
is killed. Production direct access still violates that contract at the documented
abandoned-lock/registration cuts. The experimental owner process keeps native
transaction resources out of application-worker processes and passes selected
client-kill tests without resetting survivor mappings. This does not establish
arbitrary native-engine-failure isolation, service/host-crash recovery, or durable
exactly-once message delivery. Its bounded outcome cache is volatile; an unknown
commit outcome never authorizes replay.

The new TLA+ model treats native calls as atomic, assumes accepted abstract commits
succeed, and uses stated fairness assumptions for conditional survivor progress.
It is not a Rust refinement proof. The existing component pilot and frozen-boundary
checks retain their original scope; reviewing a local boundary lock does not promote
that lock into the trusted baseline or complete P1.

The final audit confirms 37 unchanged production Rust files and eight unchanged
original Extended Crucible files. Existing proof logic is unchanged. The coverage checker adds one line to enroll
the new service protocol directory in change detection, and the local boundary
lock has been refreshed. The reviewed local 370-file lock passes; the independently
extracted original baseline checker still rejects the expanded boundary, as it
should. Final machine-readable scope and archive integrity are recorded in
[final-review.json](final-review.json) and [SHA256SUMS](SHA256SUMS).

## Qualification limits

The 50 ms p99 budget is an experimental policy, not a known HyperFeed SLA. Short
rate-grid cells identify tested operating points, not maximum sustainable capacity.
Metrics runs require exact full-history companions, but those companions do not
prove the unrecorded histories. Missing/deferred counts are not counts of every
effectless message.

PostgreSQL runs directly on this WSL host over a private Unix socket, with fsync
enabled and asynchronous acknowledgements. Both adapters perform an active normal
drain. Matched crash-loss and dependent-transaction recovery contracts remain open.
CPU/memory budgets are recorded, not enforced as equivalent allocations. Arena
high-water and PostgreSQL relation bytes are different gauges and cannot establish
a memory advantage. Loopback TCP is functional evidence only; no physical two-host
MMHF measurement is included.
