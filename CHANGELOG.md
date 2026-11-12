# Changelog

Aerostore has no tagged release yet. All crates are at version 0.1.0, and
v0.1.0 will be the first release. Until then this file summarizes the project's
history by milestone. Commit hashes refer to this repository. The
[history map](evidence/history-commit-map.tsv) translates them to the
[archived original history](https://github.com/zpconn/aerostore-archive).

## Unreleased

### Public relaunch (October–November 2026)

- Rewrote the README and added an [architecture guide](docs/architecture.md),
  a [bugs-found page](docs/bugs-found.md), a [glossary](docs/glossary.md), a
  [documentation index](docs/README.md), contributing guidelines and citation
  metadata.
- Corrected stale layout-version, Loom-case and WAL-ordering statements in
  older guides.
- Published this repository with history rewritten to remove about 9 GB of
  campaign evidence. The original history and evidence remain in the archive
  repository, and the scripted, re-runnable rewrite and its commit map are
  committed.
- Added an evidence catalog with per-campaign SHA-256 manifests and a verifying
  fetcher (`scripts/fetch_evidence.py`).
- Added green `CI` and `Verify` workflows. Verify runs the 72-check component
  pilot against a committed proof-input lock, with anchored comparisons and
  owner review of proof-input changes.

### Service capacity and performance research (September 25–30, 2026)

- Added the Contention Crucible and its serial-witness oracle, the qualification
  harness, and calibrated, affinity, maintenance-sweep and rolling workloads.
- Added the database-owned service prototype. It passes client-kill tests at
  every tested protocol cut; direct mode does not yet meet the worker-failure
  availability requirement.
- First accepted capacity comparison: 3,072 vs 640 messages/s with 16 workers
  (4.8×).
- Current comparison: **6,400 vs 704 messages/s with 24 workers (9.09×)**, with
  matching correctness companions. Both rates are lower bounds.
- Adopted a dependency-capture prefix search that cuts fresh broad lookup time
  by about 97% in microbenchmarks, with a proof of the skipped suffix.
- Added optional ordered publication policies for due-time and expiry indexes.
- Rejected, with evidence retained: two frame-coalescing candidates and two
  socket-write candidates.
- Added a paired-screen iteration loop, so candidates are screened before
  sustained trials.

### Transactional correctness and verification (September 22–24, 2026)

- Fixed structural corruption under sustained churn: detached-predecessor
  insertion, mutable published keys, commit/index reordering, allocation
  ownership, posting reclamation and collector starvation (`f993cf2`).
- Made secondary indexes transactional: predicate-bucket stamps, atomic
  row/index publication, and explicit retry for old snapshots (`6cb7254`).
- Repaired WAL publication order, the checkpoint cut, mixed-stream dependencies,
  poison admission and a duplicate primary-key insertion race (`5760a1a`).
- Clamped caller-supplied vacuum horizons in the public API (`b95d093`).
- Recovered a 7% sustained-Crucible regression with cached skiplist removal
  (`f3584f8`).
- Added the verification workspace: Verus proofs over production functions, an
  independent Lean chain, TLA+ models, Loom checks on the production lock, the
  P0 API audit, a claim ledger and an assumptions register.
- Added Extended Crucible, with reference-model replay and native contract
  probes, and the worker-failure contract probes.
- Shared-memory layout 5 and boot metadata 7. Older mappings require a cold
  rebuild.

### Initial engine (February 25 – March 4, 2026)

- Shared-memory arena with relative pointers, and process-shared MVCC/OCC
  transactions, later partitioned with hybrid hot-row handling.
- Shared-memory skiplist secondary indexes with bound-seeking range scans.
- STAPI query parser, rule-based planner and index-aware execution.
- WAL with synchronous and asynchronous commit, delta-encoded updates,
  checkpoints and recovery. Warm restart from a tmpfs mapping.
- Cross-process vacuum with shared free-list recycling.
- Tcl extension with `FlightState` ingest and search.
- The original Crucible benchmark against PostgreSQL. Early 60-second runs
  measured 10–12×. September's sustained 2 GiB runs measured 7.0–7.7× after the
  structural repairs, and about 6.1× once transactional indexes were added.
  The reports do not attribute the change to individual causes.
