# Aerostore overhaul plan

> Prepared 2026-10-01 by Claude from a read-only review of this repository. Owner: Zach Conn. Executor: Astra; any capable coding agent can follow it.

**Goals**

1. Turn Aerostore from a lab notebook into a polished, credible project. Its story: a database engine built with AI, made trustworthy by verification, tests and benchmark sandboxes.
2. Leave behind a repository where AI agents can keep doing autonomous performance research, with correctness enforced on every iteration.

## Contents

- How to execute this plan
- Decisions
- Why this order
- Target end state
- Global guardrails
- Phase 0: Preflight and inventory
- Phase 1: Honest, green CI
- Phase 2: History rewrite and evidence externalization
- Phase 3: Separate build output from run output; test tiers; sandbox crate; one entry point
- Phase 4: Verification re-baseline and engine restructure
- Phase 5: Verification workspace
- Phase 6: The autonomous research loop
- Phase 7: Story, docs and project files
- Phase 8: Harden the claims through the loop, then release
- Appendix A: Current state (measured 2026-10-01)
- Appendix B: Owner checkpoints
- Appendix C: Execution log

## How to execute this plan

**Before you start**
- Read the whole plan first.
- The facts in Appendix A were measured on 2026-10-01. Re-verify any fact before acting on it. If one turns out to be wrong, correct this plan and note it in the execution log.

**Order and progress**
- Execute phases in order. Within a phase, tasks may run in parallel unless the phase says it is sequential.
- Each task has **Done when** criteria. Tick a checkbox only once its evidence is in the execution log: command output, a CI run URL or a PR link.

**Checkpoints**
- 🛑 marks an owner checkpoint. Stop, summarize what you will do and what it changes, and wait for explicit approval in the current conversation.
- Appendix B lists every checkpoint.

**Changes**
- Make changes as small, reviewable PRs on branches.
- Never push directly to `master` unless a 🛑 step says so.

**Disk and evidence**
- `AGENTS.md` governs disk usage and evidence preservation throughout, even when that is inconvenient.
- Until Phase 3 lands, `target/` holds evidence. Do not run `cargo clean`, `rm -rf target` or `git clean -fdx`.

**Hand-off**
- Keep Appendix C (the execution log) current. It is the hand-off record between sessions.

## Decisions

### D1: Rewrite git history (decided)

**Decision:** rewrite git history to remove bulk evidence, preserving the original history in an archive repository.

**Consequences:** this is Phase 2. Old commit hashes keep resolving in the archive, and an old-to-new commit map is published.

### D2: FlightAware/HyperFeed internal detail (deferred)

**Question:** how much FlightAware/HyperFeed internal detail stays public.

**Consequences while deferred:**
- Do not add, remove or reword that content (guardrail G4).
- The Phase 2 rewrite does not redact anything. If D2 later calls for redaction, a second rewrite will be needed, so the rewrite must be scripted and re-runnable.
- Do not publish evidence to immutable venues such as Zenodo until D2 is made.

### D3: The AI-built story (decided)

**Decision:** the story emphasizes that Aerostore was built with AI and made trustworthy by verification, tests and benchmark sandboxes. The finished repository must support further autonomous AI performance research.

**Consequences:** the README's "How it was built" section, the authorship conventions (G7), and the research loop (Phase 6).

## Why this order

1. **CI first**, so the new repository launches green.
2. **History rewrite early.** Every later step benefits from a small repository and a settled evidence policy.
3. **Infrastructure before restructuring.** Build/run separation, test tiers, the sandbox crate and the single entry point come first, so that refactors are checked by fast, reliable gates.
4. **Proof tooling decoupled from file paths before any code moves.** Today even `cargo fmt` invalidates proof receipts.
5. **The research loop after the restructure.** Its protected and mutable paths can then refer to final locations.
6. **Docs last.** They then describe the final layout. The exception is a launch README in Phase 2.
7. **Hardening runs through the loop**, as the loop's first real use.

## Target end state

### Repository layout

```
aerostore/
├── crates/
│   ├── aerostore_core/        engine: storage, txn, index, wal, query, maintenance, Database facade
│   ├── aerostore_verified/    proved kernels used by the engine
│   ├── aerostore_macros/      #[derive(ShmRow)] (repurposed from #[speedtable])
│   ├── aerostore_tcl/         Tcl extension and the STAPI query front end
│   └── aerostore_crucible/    benchmark sandboxes: workloads, oracles, PostgreSQL adapter, service, runners
├── xtask/                     `cargo xtask …`: the single entry point for humans, CI and agents
├── tools/                     Python orchestration (stdlib-only) and its tests
├── verification/              proofs, models, specs and ledger, organized by engine property
├── research/                  autonomous research loop: protocol, gates, baselines, ledger, backlog
├── evidence/                  campaign catalog and small headline summaries; bulk data lives outside git
├── docs/                      architecture, guides, benchmarks, verification, reports, glossary
├── AGENTS.md                  operating manual for agents
├── README.md  CHANGELOG.md  CONTRIBUTING.md  CITATION.cff  LICENSE
└── Cargo.toml  Cargo.lock  rust-toolchain.toml  .gitattributes  .mailmap
    .github/workflows/{ci,verify,nightly}.yml
```

Three directories stay outside git (ignored):
- `target/` holds Cargo output only and is always safe to delete.
- `.tools/` holds the pinned verification toolchains.
- `runs/` holds every run's outputs.

### Properties

- A fresh clone is under 100 MB. `cargo xtask check && cargo xtask test` passes on a clean Ubuntu machine with the documented prerequisites.
- CI is green on `master`, with badges. Formatting, clippy (`-D warnings`), docs and the fast test tier are all blocking.
- The README is at most 200 lines. It covers the pitch, results with caveats beside them, how it works, how it was built (D3), verification and the bugs it found, a quick start for Rust and Tcl, reproduction steps, and a docs map.
- Every published number is generated from evidence summaries. No results are typed by hand.
- The engine crate has a curated public API and module docs, no dead code from earlier engine generations, and sealed `unsafe` boundaries.
- Proof tooling binds to symbols, not file paths, so formatting and file moves don't invalidate proofs.
- `cargo xtask experiment gate` returns one machine-readable verdict for a candidate change. It runs static checks, tests, verification, correctness oracles and a paired performance screen.
- The loop demonstrably rejects a planted bug, a planted slowdown and an attempt to edit a protected judge.

## Global guardrails

**G1. Evidence.**
- Follow AGENTS.md.
- Evidence means receipts, manifests, histories, logs, recorded executables, source snapshots and negative controls. Never delete, move or edit any of it without 🛑 approval.
- Legacy evidence stays at its current paths on the owner's machine. Remove it from git with `git rm --cached` so those local paths still resolve.

**G2. Behavior preservation.**
- Restructuring PRs (Phases 3–5) must not change transaction semantics or on-disk formats.
- Each such PR must pass the fast test tier, the verification pilot, the quick oracles, and a paired performance screen classified neutral or better.
- Once the research ledger exists, record each one there as a `refactor` entry.

**G3. Never weaken a judge to get a pass.**
- Do not delete or loosen tests, specs, oracles, thresholds or negative controls to make a change pass.
- If a judge is wrong, fix it in a separate PR that says so, with owner review.

**G4. D2 content.** Until D2 is decided, keep FlightAware/HyperFeed operating details and attributions exactly as they are:
- Move such docs verbatim. Adding a header and mechanically rewriting links is fine.
- Keep the substance of the existing README statements.
- Introduce no new internal figures.
- Find the affected text with: `git grep -n -i -E "flightaware|the architect|zach|operator|deployment|mmhf|flights per day|stored statements|chicago|user's request"`

**G5. Honesty.**
- Keep caveats, failed runs and negative results. Consolidate them; don't cut them.
- Report numbers at meaningful precision (9.0 ms, not 9.014991 ms).

**G6. Outward-facing actions.** These happen only at 🛑 checkpoints:
- pushes to `master`, force-pushes and tags;
- renaming, creating or archiving repositories;
- releases and GitHub settings changes;
- issues filed in other projects.

**G7. Authorship (D3).**
- Commits made by agents use the owner as author, with a `Co-Authored-By: <agent> (<model>) <address>` trailer. Research-loop commits also carry `Experiment: <id>`.
- This checkout's repository-level identity is currently `Codex <codex@local>`. Set the identity explicitly on every commit until Phase 7 fixes the config.
- The owner may change this convention at the Phase 7 checkpoint.

## Phase 0: Preflight and inventory (read-only)

- [x] **0.1 Measure the current state.**
  - Run `df -h . /mnt/c`, `du -xhd1 target docs .git`, `git count-objects -vH`, `gh repo view` and `gh run list --limit 100`.
  - Create `runs/` and add `/runs/` to `.git/info/exclude` for now; Phase 2 adds it to `.gitignore`.
  - Write the results to `runs/inventory/<date>/state.json`.
- [x] **0.2 Inventory the evidence.** Cover every campaign:
  - tracked: `docs/bench_data`, `docs/verification_data`, `docs/worker_failure_data`;
  - local and ignored: `docs/bench_data/sustained_capacity_2026-09-28/`, `docs/bench_data/hyperfeed_socket_write_2026-09-30/evidence/`;
  - every `target/<campaign>` directory, and `aerostore_core/target/`.

  For each campaign, record:
  - size and file count;
  - whether it has a manifest;
  - the receipts' local-only references: count, bytes, and whether each file is present and hash-valid;
  - which docs cite it;
  - which headline claims depend on it.

  Also classify `target/` content as evidence or Cargo intermediates, following `docs/disk-space.md`. Report the reclaimable bytes and delete nothing.
- [x] **0.3 Inventory the coupling** between code paths and tooling:
  - Hard-coded source paths in `verification/`, `scripts/` and `.github/`. 67 files mention `aerostore_core/src/`, and 49 files contain about 130 references to `occ_partitioned.rs`.
  - Frozen-boundary entries and the P0 inventory digests.
  - Commit hashes cited in docs and receipts.
  - Scripts that force their outputs under `target/`, e.g. `scripts/verify_formal.py:205` and `scripts/iterate_hyperfeed.py:202`.
  - Bench target names hard-coded in scripts.
- [x] **0.4 Find the headline evidence gap.** List what is missing to make the campaigns behind published numbers self-contained, with file sizes. Cover at least:
  - `hyperfeed_queue_profile_2026-09-30` and `hyperfeed_socket_write_2026-09-30-v2`, which reference 823 and 3,183 local-only files;
  - `transactional_indexes_2026-09-22`;
  - the 16-worker sustained-capacity evidence.

🛑 **Checkpoint 0.** Present an inventory summary to the owner:
- sizes;
- reclaimable intermediates;
- the headline evidence gap;
- a proposed publication scope for task 2.6.

**Done when:** the inventories exist under `runs/inventory/` and the owner has reviewed the summary.

## Phase 1: Honest, green CI

Goal: the repository that launches in Phase 2 starts with green checks. Today the only workflow has run 38 times: 34 failed, 4 were cancelled, and none passed.

- [ ] **1.1 Add `.github/workflows/ci.yml`**, triggered by PRs and by pushes to `master`.

  Setup:
  - Pin actions by SHA (the existing convention) and use `permissions: contents: read`.
  - Check out with `fetch-depth: 1` and `filter: blob:none`. Until Phase 2 removes them, use a non-cone sparse checkout that excludes `docs/bench_data/` and `docs/verification_data/`.
  - Add a Rust cache.
  - Install the prerequisites listed in the README: tcl-dev, clang, libclang-dev and pkg-config.

  Jobs:
  - `cargo build --workspace --all-targets --locked`
  - `cargo test --workspace --lib --locked`
  - `cargo test -p aerostore_macros`
  - `cargo doc --workspace --no-deps`
  - the Python unit tests that need no binaries or databases
  - `python3 -m compileall scripts verification`
  - Report-only (`continue-on-error: true`) until task 4.2: `cargo fmt --check` and clippy.

  **Done when:** the workflow is green on a PR and on `master`.
- [ ] **1.2 Fix the formal gate** (`.github/workflows/formal.yml` and `scripts/check_formal_coverage.py`).
  - **Build the frozen set from `git ls-files`.** Today it uses a filesystem `rglob` (`check_formal_coverage.py:41–45`), which picks up untracked files.
  - **Narrow the boundary to genuine proof inputs:**
    - engine sources (`aerostore_core/src`, `aerostore_verified/src`);
    - Cargo manifests and the lock file;
    - verification specs, contracts, ledger and toolchain pins;
    - the verification tooling.

    Exclude benches, tests not cited in `verification/claims.toml`, benchmark scripts and docs. Today any edit to a bench or script fails the gate.
  - **Make boundary changes a reviewed change, not a failure.**
    - The job regenerates the lock and fails only if the committed lock is inconsistent.
    - It lists the changed proof inputs in the job summary.
    - Branch protection and CODEOWNERS require owner approval for lock changes.
    - Keep the strict anchored comparison, which loads the checker from the base commit, for experiment PRs (Phase 6).
  - **Make unanchored bootstrap runs informational** rather than failing.
  - **Move the three inline Python heredocs into tested scripts.** Load the anchoring script from the base commit, as the checker already is.
  - **Speed up and tidy the workflow:**
    - Use a shallow checkout plus `git fetch --depth=1 origin <base>`.
    - Cache the toolchains, keyed by the hashes of their pins.
    - Give jobs plain names.
    - Rename the workflow to `verify.yml`.
    - Trigger it on PRs that touch proof inputs, pushes to `master`, a nightly schedule and manual dispatch.

  🛑 **Checkpoint 1.** The PR that changes the gate fails the old gate by design, because the base commit's checker rejects lock changes. The owner reviews the boundary diff and merges the PR.

  **Done when:** the verify workflow is green on `master`, and a docs-only or bench-only PR doesn't trigger it.
- [ ] **1.3 Delete the stale local branches** `diagnose/index-insert-failures-crucible` and `wip/sustained-churn-pressure-fix`. Both are fully merged and were last touched in March. They remain in the archive, and only `master` is pushed to the new repository.

## Phase 2: History rewrite and evidence externalization (D1)

Goal: a small public repository whose history no longer contains bulk evidence. The full original history and all evidence remain available in an archive.

Approach:
- Rename the current GitHub repository to `zpconn/aerostore-archive`. Nothing is re-uploaded, so old hashes and all evidence stay resolvable there.
- Push a rewritten, small history to a new `zpconn/aerostore`.

Evidence entered history in `91d4afce` (2026-03-03). That commit and the 56 after it (57 of the 119 on `master`) change hashes; the 62 earlier commits keep theirs.

- [ ] **2.1 Script the rewrite** in `tools/history/rewrite.py` (or `.sh`), documented in `tools/history/README.md`.

  The script must be fully re-runnable, driven by a config of removal rules, so a future D2 redaction is only a config change. On a fresh mirror clone it:
  - runs `git filter-repo --invert-paths --path docs/bench_data --path docs/verification_data --path docs/worker_failure_data`, plus any configured rules;
  - writes a report: new pack size, commits rewritten, largest remaining blobs, and the old-to-new commit map from `.git/filter-repo/commit-map`;
  - asserts the **invariant**: the rewritten `master` tree is byte-identical to the source `master` tree.

  It requires `git-filter-repo` (from pip or apt).
- [ ] **2.2 Prepare the externalization commit** on a branch in the current repository. Do not push it to the archive.
  - **`evidence/README.md`** explains:
    - what the evidence is, where it lives, and how to fetch and verify it;
    - that commit hashes in receipts dated before the rewrite refer to the archive, with a pointer to the commit map.
  - **`evidence/catalog.json`** has one entry per campaign with:
    - id, date and kind;
    - status: current, superseded, failed or diagnostic;
    - the claims it supports;
    - its source: `{"repo": "zpconn/aerostore-archive", "ref": "archive/pre-rewrite", "path": …}`;
    - manifest SHA-256, bytes and file count.
  - **`evidence/<campaign>/`** holds byte-identical, hash-checked copies of the small summary files behind published numbers, such as `workers24-accepted-comparison01.json` for the 9.09× result.
  - **Remove the evidence from the index.** Run `git rm -r --cached docs/bench_data docs/verification_data docs/worker_failure_data`. The files stay on disk at their original paths (G1). Add those paths to `.git/info/exclude`.
  - **Rewrite links** in the README, `docs/*.md` and the verification docs that point into those directories. Point them at archive URLs (`https://github.com/zpconn/aerostore-archive/blob/archive/pre-rewrite/<path>`) or at catalog entries. Rewrites must be mechanical only (G4).
  - **Fix links that never worked publicly:**
    - 3 absolute `/home/zpconn/...` links in `docs/crucible_allocator_telemetry_2026-03-03.md`;
    - 41 links into the gitignored `target/`: 24 in `docs/hyperfeed_commit_phases.md`, 15 in `docs/hyperfeed_predicate_lock_experiment.md`, and 2 in the pause checkpoint note.

    Point these at the copies published in task 2.6, or label them "local-only evidence".
  - **Add a link checker** (`tools/check_links.py` or lychee) that passes on the new tree, and add it to `ci.yml`.
  - **Write `scripts/fetch_evidence.py`** (it moves into `tools/` in Phase 3).
    - It makes a shallow, partial, sparse clone of the archive: `git clone --depth 1 --branch archive/pre-rewrite --filter=blob:none --sparse`, then `git sparse-checkout set <campaign path>`.
    - It verifies every file against the campaign's manifest and places the result under `runs/evidence/<campaign>/`.
    - Before the archive exists, test it against the local repository through a `file://` URL.
  - **Update `.gitignore`.** Move the two campaign-specific entries into `.git/info/exclude`, and add `/runs/` and `/.tools/`.

  **Done when:** the tree has no evidence payloads, the link check passes, and fetching works against the local source.
- [ ] **2.3 Dry run.** Run the rewrite script on a scratch mirror and produce its report.
- [ ] **2.4 Write README v1**, the launch README: at most 200 lines, following the outline in task 7.1.
  - Build it from existing content.
  - Include the results table with its caveats beside it, and a first "How it was built" section (D3).
  - Follow G4.

🛑 **Checkpoint 2.** Present to the owner:
- the dry-run report (size and invariant result);
- README v1;
- the exact list of GitHub operations in task 2.5.

Then wait for approval.

- [ ] **2.5 Execute the rewrite.** These steps are sequential.
  1. **Back up.** Check disk space, then make a local mirror backup of the original repository outside the working tree, e.g. `~/aerostore-archive.git` (about 6.6 GiB).
  2. **Tag.** Create an annotated tag `archive/pre-rewrite` at the current `origin/master` and push it.
  3. **Rename.** Rename `zpconn/aerostore` to `zpconn/aerostore-archive` with `gh repo rename`. Set its description to point to the new repository, and disable its Actions.
  4. **Rewrite.** Merge the externalization branch into local `master`. Run the rewrite script for real on a fresh clone of the local repository, and verify the invariant.
  5. **Create the new repository.** Create a new public `zpconn/aerostore` with `gh repo create`, and push only the rewritten `master`. Then configure it:
     - default branch;
     - branch protection requiring `ci` and `verify`;
     - read-only default permissions for Actions;
     - description and topics (text in task 7.7).
  6. **Publish the commit map.** Commit `evidence/history-commit-map.tsv` (old to new) in a follow-up PR.
  7. **Repoint the working directory.**
     - `git remote set-url origin …`, `git fetch`, then `git checkout -B master origin/master`.
     - The trees are identical, so no files should change. Verify that `git status` is clean and that ignored and untracked evidence is untouched.
     - Delete the old local branches.
  8. **Verify:**
     - a fresh clone is under 100 MB;
     - `ci` and `verify` are green on the new repository;
     - three campaigns, including the 9.09× campaign, fetch from the archive and pass hash verification;
     - five old commit hashes cited in the docs resolve in the archive.
  9. 🛑 **Reclaim and archive, only after step 8 passes.** Reclaim the old local objects with `git reflog expire --expire=now --all && git gc --prune=now`, then mark the archive repository as archived (read-only).
- [ ] **2.6 Publish the headline evidence that so far exists only locally**, within the scope agreed at Checkpoint 0.
  - Create `zpconn/aerostore-evidence`, containing only releases and a README.
  - Make one release per campaign. Package each as `tar.zst` archives split so every asset is under 2 GiB, with a SHA-256 manifest per asset.
  - Extend `catalog.json` with `source: release` entries.
  - Because of D2, use GitHub Releases, which can be deleted, rather than Zenodo.
  - 🛑 This is outward-facing and needs approval.

  **Policy from here on:** campaign payloads never enter the main repository. They go to `aerostore-evidence` releases and the catalog.

**Done when:** every check in step 2.5.8 passes.

## Phase 3: Separate build output from run output; test tiers; sandbox crate; one entry point

- [ ] **3.1 Use three roots.**
  - `target/`: Cargo output only.
  - `.tools/`: the pinned toolchains, overridable with `AEROSTORE_TOOLS_DIR`.
  - `runs/`: every run's output, overridable with `AEROSTORE_RUNS_DIR`. Each run lives in `runs/<kind>/<UTC>-<shortsha>-<slug>/`, containing `manifest.json`, `bin/`, `logs/` and `results/`.

  Implementation:
  - **Self-contained runs.** Copy binaries into the run as `bin/<sha256>`, so receipts reference only files inside their own run directory, by relative path. `scripts/hyperfeed_screen_capture.py` already copies executables; generalize that.
  - **One manifest schema, `aerostore.run/v1`.** Record:
    - the git SHA plus a hash of any uncommitted diff;
    - `rustc -Vv`, the profile, features and RUSTFLAGS;
    - a host fingerprint: CPU, governor, SMT, kernel, WSL;
    - arguments and seeds.

    This replaces today's mix of `schema`, `schema_version`, `format_version` and `version`.
  - **Change every script that refuses outputs outside `target/`** (inventory 0.3) to default to `runs/`.
  - **Clean up isolated verification builds automatically.** `scripts/check_lock_models.py` and `verification/*_native/run.py` keep fresh, isolated targets, but put them inside the run. They delete Cargo intermediates once binaries are copied and hashed, automating what `docs/disk-space.md` describes by hand.
  - **Add `cargo xtask runs du|gc|preflight`.**
    - `preflight` checks Linux free space and the WSL host volume against a reserve (default 30 GiB, configurable).
    - `gc` removes only the Cargo intermediates of completed runs. It never removes manifests, logs, histories or `bin/`.
  - **Leave legacy evidence under `target/` where it is (G1).**
  - **Move the toolchains only with approval.** 🛑 Moving `target/verification-tools` to `.tools/` (leaving a compatibility symlink) needs approval, because AGENTS.md treats it as a retained tool.
  - **Shrink the runbooks.** Reduce `docs/disk-space.md` to a short "Running large campaigns" guide, and update the disk section of AGENTS.md to match while keeping its principles.

  **Done when:** a verification pilot and a benchmark screen both run end to end and write only to `runs/` and `.tools/`.
- [ ] **3.2 Define test tiers** in `.config/nextest.toml`, run with `cargo xtask test --tier <fast|full|perf|stress>`.
  - **`fast`** is the default. It runs in CI in about 15 minutes and contains no wall-clock assertions.
  - **`perf`** takes the timing gates, e.g. `aerostore_core/tests/wal_ring_benchmark.rs:49`, which requires async to be at least 10× faster than sync. Rename benchmark-style test files to `perf_*`.
  - **Fork tests run serially.** 13 test files `fork()` from the multithreaded test harness. Put them in a serial test group, and over time migrate them to the re-exec pattern that `tests/worker_failure_contract.rs` already uses.
  - **Add a shared `tests/common/` module:** self-cleaning temp and `/dev/shm` paths, `wait_until` and a child re-exec helper. Remove the copy-pasted versions; `wait_until` alone exists in 6 files with two argument orders.
  - **Fix fragile tests:**
    - fixed temp file names in `wal_ring_benchmark.rs`;
    - outputs relative to the working directory, which create `aerostore_core/target/`;
    - sleep-based synchronization, where feasible.
  - **Tcl tests:** stop running a nested `cargo build`. Build the cdylib through the harness, or move these tests to the `full` tier.

  **Done when:** the fast tier is blocking in CI, and the `full`, `perf` and `stress` tiers are documented in CONTRIBUTING.
- [ ] **3.3 Create the benchmark sandbox crate `aerostore_crucible`** (`publish = false`).
  - **Move the harness.** Move `aerostore_core/benches/{hyperfeed_crucible.rs, contention_crucible/, extended_crucible/, support/}` into `src/` (library) and `src/bin/` (`crucible`, `extended_crucible`, `contention_crucible`). Move `scripts/predicate_capture_microbench.rs` there too.
  - **Move the coupled tests.** 12 test files pull bench modules in through `#[path = "../benches/…"]`. Make them ordinary tests in the new crate (`use aerostore_crucible::…`), and delete their fake module trees and blanket `#![allow(dead_code, unused_imports)]`.
  - **Slim down core.** `aerostore_core/benches/` keeps only criterion microbenchmarks, and postgres and testcontainers leave core's dev-dependencies.
  - **Keep measurements comparable.** Add a `[profile.crucible]` that inherits the current bench profile (codegen-units = 1, thin LTO).
  - **Remove the criterion misuse.** `hyperfeed_crucible` runs the whole workload during setup, then has criterion time `black_box(tps_ratio)` (`aerostore_core/benches/hyperfeed_crucible.rs:592`). Emit a JSON report instead. Port `scripts/check_crucible_2g_120_vs_240.sh` to read that JSON; today it parses table columns by position.
  - **Give every sandbox one configuration source:**
    - CLI flags instead of environment variables;
    - a required `--output`;
    - `--print-defaults`, which emits JSON so the Python tooling reads defaults instead of copying them. Today they are duplicated across `qualify_hyperfeed.py`, `run_remote_contention.py` and the Rust code.
  - **Update callers** that hard-code bench target names, and the source fingerprint in `qualify_hyperfeed.py` (`snapshot_sources()`).

  **Done when:** the sandboxes build and run from the new crate, their embedded tests run once, and a paired screen against the pre-move binary is neutral.
- [ ] **3.4 Build the Python `tools/` package and `cargo xtask`.**

  The Python package:
  - **Packaging.** Add `tools/pyproject.toml`: Python ≥ 3.12 everywhere, stdlib-only at runtime, and pytest plus ruff for development.
  - **Layout.** Package `aerotools`, with:
    - `common/`: hashing, atomic JSON, manifests, process-group supervision, build→copy→hash, host fingerprint, disk;
    - `bench/`, `verify/`, `evidence/` and `history/`.
  - **Deduplication.** Remove duplicated helpers; for example, `digest()` is defined in 12 scripts.
  - **Tests.** Consolidate them under `tools/tests/`; pytest runs the existing unittest classes. Use the markers `binary`, `postgres` and `slow`.
  - **Transition.** Leave thin shims in `scripts/` for documented commands until the docs are updated. Keep the path of any script that CI loads from the base commit stable, or change the loader in the same PR.

  The `xtask/` crate is a thin Rust dispatcher with stable subcommands and `--json` output. Exit codes: `0` pass, `1` fail, `2` inconclusive, `3` infrastructure error.

  | Area | Subcommands |
  |---|---|
  | Checks and tests | `check`, `test --tier` |
  | Verification and oracles | `verify --profile <pr\|pilot\|full> [--impacted]`, `oracle --quick\|--full` |
  | Benchmarks | `bench screen\|qualify\|calibrate` |
  | Research loop | `experiment new\|gate\|guard\|promote\|record\|search\|selftest` |
  | Runs and evidence | `runs du\|gc\|preflight`, `evidence fetch\|verify\|pack` |
  | Generated content | `results render`, `docs check-links` |

  **Done when:** the README, CONTRIBUTING and CI use only `cargo xtask …` commands.

## Phase 4: Verification re-baseline and engine restructure

**This phase is sequential.** The proof tooling copies functions out of production files by path, and `verification/frozen_boundary.json` hashes file bytes, so today even `cargo fmt` invalidates receipts. Task 4.1 removes that coupling, which makes the rest of the phase cheap and safe.

Tasks 4.6 to 4.8 change APIs and safety boundaries. They do not change transaction semantics or on-disk formats.

- [ ] **4.1 Make the proof tooling path-agnostic.**
  - Write a shared source locator, either in xtask using `syn` or as one Python module used by every generator. It resolves proof-bound items by crate, module path, item name and impl type, instead of by file path and line.
  - Digest proof-bound items by their normalized token stream rather than file bytes. The digest ignores whitespace, comments and position. Semantic edits still change it; formatting and moves don't.
  - Stop generators from modifying each other's globals at import time (`verification/lifecycle/generate.py:17`, `verification/predicate/generate.py:27`).

  🛑 **Checkpoint 4.1.** Re-baseline once; the owner reviews.

  **Done when:** running `cargo fmt --all` on the whole workspace leaves verification green with no re-baseline.
- [ ] **4.2 Set the formatting and lint baseline.**
  - **Formatting.** Run `cargo fmt --all` and make fmt blocking.
  - **Toolchain.** Add `rust-toolchain.toml`: 1.93.1 with rustfmt and clippy.
  - **Workspace metadata.** Add `[workspace.package]` (license, repository, description, rust-version, edition), and use `edition.workspace = true` in every crate.
  - **Dependencies.**
    - Add `[workspace.dependencies]`, starting with serde and tokio; tokio is "1.44" in core and "1.48.0" in tcl.
    - Remove unused dependencies.
  - **Lints.**
    - Add `[workspace.lints]`: `unsafe_op_in_unsafe_fn`, `missing_debug_implementations`, `clippy::undocumented_unsafe_blocks` and `clippy::missing_safety_doc`.
    - Fix the warnings, then make clippy `-D warnings` blocking.
  - **Generated files.** In `.gitattributes`, mark the generated `*.verus.rs` files and the Aeneas outputs as `linguist-generated`. The 24 `*.verus.rs` files are 34% of the repository's Rust bytes.
- [ ] **4.3 Delete the dead engine generations.**
  - **Tag first.** Create the tag `pre-legacy-removal` locally; push it at the next 🛑.
  - **Move live pieces out.** Move `TxId` and the TSV column decoder, which the Tcl bridge uses, into live modules.
  - **Delete:**
    - the modules `arena`, `mvcc`, `txn`, `query`, `wal`, `watch`, `recovery`, `wal_logical`, `occ_legacy` and the `occ` alias;
    - `IntoIndexValue`;
    - `bulk_upsert_tsv`'s `DurableDatabase` path;
    - tests that exercise only removed code;
    - dependencies used only by removed code.
  - **Why this is safe.** None of this has live callers. `TransactionManager`, `MvccTable`, `QueryBuilder`, `QueryEngine`, `TableWatch`, `DurableDatabase` and `LogicalDatabase` are referenced only by their own modules and tests.
  - **Why it matters.** The crate root currently exports legacy types under the obvious names, `aerostore_core::{Table, Transaction, WalRecord, ChunkedArena}`, while the live types need aliases.
  - **Regenerate** `verification/contracts/p0_inventory.json`.
- [ ] **4.4 Build the module hierarchy and a curated API.**

  ```
  storage/      arena (shm.rs), ptr (RelPtr, MmapBase), lock (shm_lock), tmpfs, boot (bootloader)
  txn/          procarray, table, commit, predicate, rows, visibility, retry, diagnostics  (from occ_partitioned)
  index/        value, secondary/ (shm_index), primary_key (from execution.rs), skiplist/ (pub(crate))
  wal/          codec (wal_delta), ring (wal_ring), writer, committer, checkpoint, recovery (recovery_delta + recover_*)
  query/        ast, row (StapiRow → Row), planner (rbo_planner + planner_cardinality), filter, exec
  maintenance/  vacuum, GC daemons
  database.rs   facade (task 4.6)
  error.rs
  sys/          pub(crate): daemons, pdeathsig, fdatasync
  ```

  - Re-export about 40 names at the root, with no renaming aliases.
  - Make internals `pub(crate)`. Where the sandboxes need them, use a `#[doc(hidden)]` `internals` module behind a feature.
  - Break the import cycles `index ↔ shm_index` and `rbo_planner ↔ execution`.
- [ ] **4.5 Split the giant files along their existing seams.** These are pure moves, checked by the token-stream digests.
  - `shm_skiplist.rs`: 5.7k lines, including a single 3,226-line `impl` block. Split into layout, posting, mutate, read, search, gc, daemon, pressure, recycle, audit and telemetry. Move its roughly 40 counters into a `SkipTelemetry` struct.
  - `occ_partitioned.rs`: 4.3k lines, 45% of them tests. Split into the `txn/` files, with tests in `txn/tests/`.
  - `shm_index.rs`: 2.4k lines. Split into key, publication, transactional, raw, retry and telemetry.
- [ ] **4.6 Add a `Database` facade** in core, covering boot, layout, recovery, committer, daemons and checkpointer.

  It replaces the ~650 lines of generic orchestration in `aerostore_tcl/src/lib.rs` (`SharedFlightDb`, from line 532). Then fix the Tcl bridge:
  - **Initialization.** Make it single and race-free, and reject re-initialization with a different directory. `ensure_database` currently races, and silently ignores a second `init` with a different directory.
  - **Safe interpreters.** `Aerostore_SafeInit` must not expose the full command set; today it just calls `Init` (`aerostore_tcl/src/lib.rs:1269`).
  - **FFI docs.** Add `# Safety` docs to the FFI helpers.
  - **Errors.** Surface checkpointer errors by adding a `tracing` facade; a detached thread drops them today.
  - **Dependencies.** Drop the unused ones.
- [ ] **4.7 Move domain-specific code out of core.**
  - **STAPI parser.** Move it to `aerostore_tcl` (or behind a `stapi` feature), keeping the query AST in core.
  - **Planner.** Use schema-provided cardinality hints instead of the hard-coded `flight_id`/`geohash`/`dest`/`altitude` ranks (`aerostore_core/src/rbo_planner.rs:98`).
  - **Error strings.** Remove the `"TCL_ERROR: …"` strings from core.
  - **Macros crate.** Repurpose `aerostore_macros` as `#[derive(ShmRow)]`, generating `ShmSafe`, `Row` and `WalDeltaCodec`. It replaces the hand-written row boilerplate in the Tcl crate and retires `#[speedtable]`, which today is used only by one legacy test.
- [ ] **4.8 Tighten soundness and error handling.**

  Soundness:
  - **The problem.**
    - `RelPtr::from_offset` (`shm.rs:173`) and `RelPtr::as_ref` (`shm.rs:212`) are both safe and generic, so safe code can produce a `&T` at any in-bounds offset.
    - `recycle_raw` (`shm.rs:794`) is a safe function that frees an arbitrary offset.
    - `OccTable<T: Copy>` accepts process-local pointers such as `&'static str`.
  - **Introduce `unsafe trait ShmSafe`** and require it for `OccTable<T>`, typed shared pointers and arena allocation.
  - **Restrict raw access.** Make raw operations `unsafe` or `pub(crate)`, and make the `ShmArena` setters and the `OccRow` header fields private.
  - **Document every unsafe site.** Put `// SAFETY:` on every unsafe block and impl, enforced by lint.

  Errors:
  - Use `thiserror`, keeping causes as typed `#[source]` fields instead of `to_string()` (e.g. `OccError::{Index, ProcArray, Allocation}`).
  - Mark error enums `#[non_exhaustive]`.
  - Add a top-level error with `is_retryable()` and `is_indeterminate()`.
  - Remove wrappers that swallow errors.
- [ ] **4.9 Write rustdoc and examples.**
  - A crate-level `//!` overview: layout, commit protocol, durability, trust model, and both execution modes.
  - `//!` docs on every module; 3 of 33 have them today.
  - `///` docs on every public item; about 5% have them today. Ratchet with `#![warn(missing_docs)]`, then deny.
  - Doctests.
  - `crates/aerostore_core/examples/quickstart.rs`, run in CI. It should show a shared table, indexes, transactions, a query, a WAL commit and a warm restart.
- [ ] **4.10 Move the crates under `crates/`**, keeping the package names. This is a pure move.

**Done when:** every PR in this phase passed the checks in G2, and CI, verification and screens are green at the end.

## Phase 5: Verification workspace

- [ ] **5.1 Organize by engine property, and separate specs from proofs.**

  Move the current directories as follows:

| Current | New |
|---|---|
| `claims.toml`, `assumptions.toml`, `refinement_campaigns.json`, `frozen_boundary.json` | `ledger/` |
| `contracts/` | `specs/` |
| `verus/`, `bridge/` | `kernels/verus/`, `kernels/bridge/` (pins move to `toolchains/`) |
| `lean/` | `lean/`, with `Kernels/` and `Protocol/` namespaces |
| `tla/`, `service_protocol/` | `models/tla/{transactions,durability,resources,service}/` |
| `concurrent`, `write_plan`, `write_admission`, `commit_data`, `commit_completion`, `planned_commit` | `proofs/commit/…` |
| `predicate`, `predicate_capture`, `predicate_composition`, `publication_slice` | `proofs/predicate/…` |
| `lifecycle`, `lifecycle_scenario`, `lifecycle_interference` | `proofs/lifecycle/…` |
| `guards`, `guard_ownership` | `proofs/locks/…` |
| `lookup`, `indexed_slice` | `proofs/read_path/…` |
| `row_publication`, `row_retention`, `row_initialization`, `storage_slice`, `postings`, `skiplist_detach` | `proofs/storage/…` |
| `p1_native`, `planning_native`, `retention_native`, `lifecycle_native`, `lookup_native`, `ordered_range` | `native/…` |
| `retry_diagnostics/`, `experiments/` | `diagnostics/retry/`, `experiments/` |

  Inside each campaign, split two kinds of content:
  - `spec/` holds contract traits, `requires`/`ensures` and statements. Phase 6 protects it.
  - `proof/` holds proof bodies and adapters. It stays mutable.

  For Lean, pin the statement (the type) of every required root by hash in `roots.json`, so proofs may change but statements can't.
- [ ] **5.2 Replace the per-campaign runners with one data-driven runner.**
  - It replaces the ~30 near-identical `run.py` files and is driven by each campaign's manifest: inputs, roots, mutants and expected outcomes.
  - Add a shared adapter library to remove duplicated generated code.
  - The pilot never runs `lifecycle_native/run.py` or `lookup_native/run.py`. Either wire them in or retire them.
- [ ] **5.3 Decide what generated output to keep.**
  - **Keep, marked as generated:** the `*.verus.rs` files, so that diffs stay reviewable.
  - **Stop committing:** the unused Charon LLBC. For `translation.json`, either check it or stop committing it.
  - **Replace:**
    - The 136 committed TLC logs, all of which contain `/home/zpconn`, PIDs and timestamps. Keep normalized fixtures plus the counterexample traces that found bugs, and make the rest CI artifacts.
    - `retry_diagnostics/occ_partitioned_94ad54b.rs`, a 165 KB copy of old source. Use a git ref and a hash instead.
- [ ] **5.4 Make the ledger and status data-driven.**
  - Add an `evidence_kind` to each claim:
    - exact-source proof;
    - extracted-method conditional proof;
    - abstract theorem;
    - model-checked, bounded, tested, or open.
  - Compute status from receipts instead of hand-set strings. Today the ledger forbids "proved", so fully proven kernels show `in_progress`.
  - Generate `verification/STATUS.md`, and have CI check that it is current.
- [ ] **5.5 Add impact analysis for the research loop.** Promote `scripts/hyperfeed_proof_impact.py`, which is advisory today, into `cargo xtask verify --impacted`.
  - It maps changed symbols to the campaigns, models and roots they affect.
  - That lets an experiment rerun only the proofs its diff touches in the inner loop. The full pilot is still required for promotion.
- [ ] **5.6 Rewrite the verification README and split the plan.**

  New `verification/README.md` outline:
  1. What this is and isn't.
  2. Key numbers, generated.
  3. How proofs connect to code, with a diagram of the three paths:
     - Verus on the exact kernel source;
     - Rust → Charon → Aeneas → Lean;
     - Verus on mechanically extracted methods under primitive contracts.
  4. Results by engine property.
  5. Bugs found, attributing each to what actually found it. For example, the primary-key race was exposed by the release test suite, not by a proof.
  6. What is trusted.
  7. How to run it, including time and disk needs.
  8. Evidence, anchoring and CI.
  9. How the research loop uses it.
  10. Roadmap P0–P6.
  11. A link to the glossary.

  Also:
  - Move the dated "expansion" paragraphs to `HISTORY.md`.
  - Split `docs/formal_verification_plan.md` into a roadmap (line 232 onward) and a history (lines 1–231).
  - Fix `verification/tla/README.md:227`, which says "eight models"; there are ten.
- [ ] **5.7 Report the Aeneas issue upstream.** Draft an issue for Aeneas: its pinned `Vec::insert` model replaces an element and rejects end insertion, unlike Rust (`verification/lean/README.md:102`).
  - 🛑 The owner files it, or approves filing it.
- [ ] **5.8 Be honest about the default path.**
  - The bucket kernels proved in both Verus and Lean are opt-in (`verified-buckets-sort`, `verified-buckets-bitmap`). The default uses `sort_unstable` followed by `dedup` (`shm_index.rs:731`).
  - Screen both features against the default:
    - if one is neutral or better, propose it as the default through the Phase 6 loop;
    - otherwise, state plainly in the docs that the default path is not the proved one.

## Phase 6: The autonomous research loop

Purpose:
- An agent proposes a performance change.
- The repository decides, mechanically and with evidence, whether the change is correct, whether it helps, and whether it may be promoted.
- The judges (tests, specs, oracles, thresholds) are protected from the agent proposing the change.

- [ ] **6.1 Create `research/`:**
  - **`README.md`:** the starting point for agents and humans, covering the loop, its commands and its rules.
  - **`protocol.md`:** the experiment protocol (task 6.3).
  - **`gates.toml`:** tier definitions, thresholds, noise settings, multi-metric tolerances and the held-out workload set. Protected.
  - **`baselines.toml`:** the accepted baseline. It records the engine commit, the binary recipe and hash, the host fingerprint, the headline metrics and evidence IDs. Protected; only promotion PRs change it.
  - **`ledger.jsonl`:** an append-only experiment registry with schema `aerostore.experiment/v1`. Fields:
    - id, dates, agent and model;
    - hypothesis and area;
    - baseline, and candidate (branch, SHA, diff hash);
    - per-tier results with run and evidence IDs;
    - metric deltas with confidence intervals;
    - verdict: `promoted`, `improved`, `neutral`, `regressed`, `failed-correctness`, `inconclusive` or `abandoned`;
    - a report link.

    Corrections are new entries that reference the old id.
  - **`backlog.md`:** open hypotheses, each with expected impact, risk and proof impact.
- [ ] **6.2 Seed the ledger and backlog from past work**, so agents don't repeat it.

  Backfill the ledger from the existing reports, for example:

| Experiment | Outcome |
|---|---|
| Frame coalescing | No consistent gain |
| Two socket-write candidates | Not promoted |
| Predicate-lock yield cadence | Neutral |
| Dependency-capture prefix | Adopted; p99 −81% at 512 msg/s |
| memfd arena | p99 −40–53% |
| Ordered due-range | Optional, not the default |
| 24 workers | 6,400 msg/s |

  Seed `backlog.md` from the open leads in the reports: predicate-lock wait bursts, page-fault spikes and transport. Baseline-fairness items, such as PostgreSQL tuning, are judge work, not engine experiments.
- [ ] **6.3 Write the protocol.**
  1. **Check prior work.** Read `research/README.md` and the backlog, and run `cargo xtask experiment search <terms>`. Don't retry a recorded idea without a new reason.
  2. **Open an experiment.** `cargo xtask experiment new --hypothesis "…"` creates the branch `exp/<id>-<slug>`, a ledger entry and a run directory.
  3. **Change only mutable paths** (task 6.4). Adding tests is encouraged.
  4. **Iterate** with `cargo xtask experiment gate --id <id>`.
  5. **Open a PR if it helps.** If the screen shows an improvement, open a PR labeled `perf-experiment` with the generated report.
  6. **Promote.** `cargo xtask experiment promote --id <id>` runs G5. 🛑 The owner approves the merge and the baseline update.
  7. **Record every outcome**, including failures and inconclusive results, with `cargo xtask experiment record`.

  **Spec-first rule.** Some optimizations change a protocol covered by TLA+, Loom or Verus contracts, such as lock order, publication order or WAL ordering. For those, the spec or model change lands first, as an owner-reviewed judge PR with its own model-checking evidence.
- [ ] **6.4 Separate the subject from the judges.**
  - **Mutable in experiments:**
    - implementation in `crates/aerostore_core/src/**` and `crates/aerostore_verified/src/**`;
    - `verification/**/proof/**`: the proof bodies and adapters needed to re-establish unchanged specs;
    - new tests.
  - **Protected (changed only through owner-reviewed judge PRs):**
    - verification: `verification/**/spec/**`, `verification/ledger/**`, `verification/models/**` and the Lean root statements;
    - tests: existing test functions anywhere, which must stay unchanged token for token (additions are allowed);
    - sandboxes: `crates/aerostore_crucible/**`, meaning workloads, oracles and the PostgreSQL adapter;
    - research config: `research/gates.toml` and `research/baselines.toml`;
    - `evidence/**`;
    - tooling and policy: `xtask/**`, `tools/**`, `.github/**` and `AGENTS.md`;
    - Cargo manifests and `Cargo.lock`, because dependency changes are a supply-chain decision.
  - **Enforcement:**
    - `cargo xtask experiment guard` runs in CI for `exp/*` branches and `perf-experiment` PRs. It executes the checker from the base commit, the existing trusted-base pattern.
    - CODEOWNERS and branch protection cover the protected paths.
  - **Strengthening judges:** allowed, but never in the same PR as an optimization.
- [ ] **6.5 Implement the gates.** `cargo xtask experiment gate` runs the tiers in order and stops at the first failure. It writes `runs/experiments/<id>/gate.json` (schema `aerostore.gate/v1`) and a summary.

| Tier | Contents | Target time |
|---|---|---|
| G0 static | fmt, clippy `-D warnings`, feature matrix (`retry-diagnostics`, `verified-buckets-sort`, `verified-buckets-bitmap`) | ≤ 5 min |
| G1 tests | nextest `fast` | ≤ 15 min |
| G2 verification | `verify --impacted`; the full pilot before promotion | ≤ 20 min |
| G3 oracles | Extended Crucible native contracts; a short contention run checked for serializability against its full history; crash/recovery suite; Loom models | ≤ 20 min |
| G4 screen | Paired, alternating A/B runs against the accepted baseline binary on the same host (reusing the design of `scripts/iterate_hyperfeed.py`). Reports effect sizes with confidence intervals for throughput, p99 including retries, memory growth and reclamation, WAL bytes, and CPU. Verdict: improved, neutral, regressed or inconclusive | ≤ 60 min |
| G5 promotion | Sustained capacity trials at the target rates (two 905 s trials per engine, as today); full-history correctness companion; resource checks; held-out workloads | hours |

  Rules:
  - **Inconclusive never counts as a pass.**
  - **No hidden regressions.** An improvement in one metric may not regress another beyond the tolerances in `gates.toml`.
  - **Held-out workloads.** G4 uses a development set of workloads and seeds. G5 adds a held-out set (different seeds, populations and mixes) listed in `gates.toml`. The ledger records every held-out run, and experiments must not iterate against held-out results.
  - **Where tiers run.** G4 and G5 need a quiet, dedicated host. They run locally, or on a self-hosted runner (🛑), and attach `gate.json` to the PR. GitHub-hosted CI runs G0–G2 and the guard.
- [ ] **6.6 Control noise and the host.**
  - `cargo xtask bench calibrate` runs A/A comparisons to measure noise and the minimum detectable effect for each host.
  - Every manifest includes the host fingerprint.
  - A benchmark lock prevents concurrent screens; extend the existing `target/hyperfeed-iteration.lock` mechanism.
  - Run the disk preflight before every run.
- [ ] **6.7 Set autonomy budgets and stop rules**, in `gates.toml` and AGENTS.md:
  - one benchmark at a time per host;
  - a disk reserve;
  - per-tier timeouts;
  - a maximum number of attempts per experiment;
  - after three consecutive infrastructure errors, stop and report;
  - if `master` fails any gate, stop all experiments and alert the owner.
- [ ] **6.8 Measure judge strength.**
  - Run `cargo mutants` weekly on the engine's hot modules, or on changed files, and measure coverage with `cargo llvm-cov`.
  - Mutants that survive in performance-critical code become items on the judge backlog.
  - Keep the existing proof mutants and negative controls.
- [ ] **6.9 Self-test the loop** with `cargo xtask experiment selftest`. Run it weekly, and before declaring this phase done.
  1. A no-op change (a comment or whitespace) passes every gate with a neutral screen.
  2. A planted semantic bug, such as skipping predicate validation at commit, is rejected. Record which tier caught it.
  3. A planted slowdown, such as a spin in the commit path, is classified `regressed`.
  4. An edit to a protected judge, such as loosening a test assertion, is rejected by the guard.
- [ ] **6.10 Rewrite AGENTS.md as the operating manual.**

  It covers:
  - orientation and commands;
  - the protocol;
  - mutable versus protected paths;
  - budgets;
  - evidence and honesty rules;
  - escalation: when to stop and ask;
  - authorship conventions (G7).

  Keep the evidence-preservation principles. The disk section shrinks because of `runs/`.

**Done when:** the self-test passes, and one real experiment (such as task 5.8) has gone through the loop end to end.

## Phase 7: Story, docs and project files

- [ ] **7.1 Write README v2**: at most 200 lines and at most 25 links.

  Section order:
  1. Tagline and badges.
  2. Pitch.
  3. **Results at a glance.** A generated table, with these caveats directly beneath it:
     - the numbers are lower bounds;
     - PostgreSQL's configuration;
     - durability differences;
     - on Crucible, Aerostore's update-only p99 is worse.
  4. **How it works.** A diagram and both execution modes:
     - direct shared memory;
     - the database-owned service. That mode carries the 9.09× result, and it is also the path to isolating worker failures.
  5. **How it was built** (D3; details below).
  6. Correctness and verification: three bullets and a link to the bugs-found page.
  7. Status and limits.
  8. Quick start for Rust and Tcl.
  9. Reproducing the headline results.
  10. Docs map.
  11. Layout.
  12. Contributing and license.

  "How it was built" should say:
  - Aerostore was built with AI coding agents. 🛑 The owner confirms which agents and models to name, and how to describe their share of the work.
  - Systems code written that way is only as trustworthy as its checks, so three kinds of guardrails grew alongside the engine:
    - proofs bound to the production source;
    - tests and oracles: serializability checking, a reference model replayed against PostgreSQL, and deterministic fault injection;
    - benchmark sandboxes: paired screens, sustained trials, retained failures and evidence manifests.
  - The defects the guardrails caught.
  - The research loop that now carries the work forward.

  Before claiming that a bug was in AI-written code, confirm who wrote the faulty lines with `git log` or `git blame` in the archive.

  Also:
  - **Narrative.** Restore the "why it's fast" narrative from `docs/archive/README-2026-09-22.md`, updated for the current design.
  - **Name.** Use one spelling everywhere. Today "Aerostore" appears 67 times and "AeroStore" 65 times; 🛑 the owner picks.
  - **Precision.** Use sensible precision (9.0 ms, not 9.014991 ms).
  - **G4 applies.**

  🛑 The owner reviews the README before it merges.
- [ ] **7.2 Restructure the docs.**

  ```
  docs/README.md          index by audience
  docs/architecture/      overview (diagram), transactions (OCC/MVCC/predicate protocol), durability, failure-model, limitations
  docs/guides/            getting-started, rust-api, tcl, running-benchmarks, benchmark-options, running-campaigns
  docs/benchmarks/        results.md (generated), methodology.md, workloads.md, postgresql-baseline.md
  docs/verification/      overview (proven / assumed / tested), bugs-found
  docs/reports/           YYYY-MM-DD-slug.md, immutable, with a Status / Question / Answer / Evidence / Superseded-by header; INDEX.md
  docs/glossary.md
  ```

  Today 26 of the 38 `docs/*.md` files are `hyperfeed_*`, 18 are experiment reports, and only about 3 describe the engine.

  Mapping:
  - Experiment reports go to `reports/` verbatim, with the header added.
  - Guides and runbooks go to `guides/`.
  - Design text goes to `architecture/`.

  Merging and banners:
  - Merge only docs that contain no D2-scoped content (G4). For example, summarize `hyperfeed_arena_capacity` → `hyperfeed_commit_phases` → `hyperfeed_queue_profile` as "the path to 9.09×" in `benchmarks/results.md`.
  - Add "Superseded by" banners to `arena_capacity`, `sustained_capacity`, `rolling_findings` and `expiry_range`.
- [ ] **7.3 Keep one source of truth for numbers.**
  - `cargo xtask results render` generates `docs/benchmarks/results.md` and the README table from `research/baselines.toml` and the evidence summaries.
  - CI fails if the generated files are stale.
  - Include the Crucible ratio history: about 10–12× in early 60 s runs and 6.1× in sustained 2 GiB runs, with the reason it changed.
- [ ] **7.4 Fix stale facts.** Each of these was verified on 2026-10-01.
  - **Layout version.** `docs/sustained_churn_correctness.md:115` and `docs/transactional_indexes.md:65` say layout 4 with boot metadata 6; the code has 5 and 7.
  - **CPU budget.** It is "enforced" in `docs/hyperfeed_arena_capacity.md:47`, but "a declaration, not enforcement" in `docs/hyperfeed_capacity_resources.md:59`.
  - **Loom case count.** `docs/nightly_perf.md:109` says five; the README says seven.
  - **GitHub description.** It says "lock-free … market/flight workloads". The engine uses mutexes and predicate locks, and "market" appears nowhere.
- [ ] **7.5 Write a glossary** of about 40 terms, each linked from its first use.
  - **Workloads and benchmarks:** HyperFeed, MMHF, STAPI/Speedtables, Crucible, Extended Crucible, contention Crucible, calibrated Crucible, qualification harness.
  - **Process:** P0–P6, gate, promotion, anchored, frozen boundary, receipt, campaign.
  - **Measurement:** full-history vs metrics run, companion, offered rate, service-unix, memfd arena.
  - **Workload concepts:** projection, housekeeping, signature affinity, ordered-due.
  - **Engine:** horizon, stamp, posting, poison.
  - **Family:** it has three meanings today; rename two of them.
- [ ] **7.6 Write the house style** into CONTRIBUTING:
  - Put the conclusion first.
  - Give each result one caveats block.
  - Put numbers in tables.
  - Use the present tense for what is true now.
  - History goes in reports.
  - Use sensible precision.

  `docs/hyperfeed_queue_profile.md` and `docs/worker_failure_contract.md` are the models.
- [ ] **7.7 Add the standard project files.**
  - **Project docs.**
    - `CHANGELOG.md`, starting at 0.1.0 with a summary of the history.
    - `CONTRIBUTING.md`: tiers, xtask, the protocol, judge PRs, style.
    - `CITATION.cff`.
  - **`.mailmap`.**
    - Map `zpconn` to `Zach Conn <zpconn@gmail.com>`.
    - Give the agent identity a clear display name, such as `Codex (AI agent) <codex@local>`. 🛑 The owner confirms the display names.
    - Keep the 62 agent-authored commits as they are; they are part of the D3 story.
  - **Git identity.** Unset this checkout's repository-level `user.name=Codex` and `user.email=codex@local`. Agents set their identity per G7.
  - **Logo.** Commit it under `docs/assets/` and render it at about 200 px.
  - **GitHub metadata.** 🛑 Set the description and topics.
    - Suggested description: "Shared-memory transactional database engine in Rust, built with AI agents and kept honest by formal verification, test oracles and benchmark sandboxes."
    - Topics: rust, database, storage-engine, mvcc, shared-memory, formal-verification, verus, lean4, tla-plus, ai-agents.

## Phase 8: Harden the claims through the loop, then release

Each item goes through Phase 6 as either an engine experiment or a judge PR. This is the loop's first real use.

- [ ] **8.1 Judge PR: a tuned PostgreSQL baseline.**
  - Keep today's configuration as a labeled baseline, and add a tuned one:
    - `shared_buffers` sized to the working set (today it is 128 MiB);
    - serialization-failure logging without the full SQL text (one 704 msg/s trial produced a 473 MB log);
    - a matched index set where semantics allow (today PostgreSQL has nine indexes against Aerostore's five secondary indexes).
  - Re-run the sustained comparison and publish both results.
  - 🛑 The owner approves the tuning choices, because fairness is a judgment call.
- [ ] **8.2 Add WAL frame checksums** (CRC32C), with defined torn-tail semantics: truncate to the last valid frame.
  - Today there are no checksums, and a torn tail is a recovery error.
  - Spec first: update the durability model, then the implementation.
  - Bump the format version.
- [ ] **8.3 Remove fork-without-exec from the library.**
  - Sites: `wal_writer.rs:261` and `shm_skiplist.rs:1736`. The third site, `wal_logical.rs:280`, is gone after task 4.3.
  - Use threads with shared-memory leader election, as `vacuum` already does, or a spawned helper binary.
  - Re-run the worker-failure contract.
- [ ] **8.4 Make a verified kernel the default** if task 5.8 found one neutral or better.
- [ ] **8.5 Release v0.1.0.** 🛑
  - Tag the release, publish a GitHub release with notes from the CHANGELOG, and add `CITATION.cff`.
  - After D2 is decided, optionally archive the evidence on Zenodo for a DOI.

## Appendix A: Current state (measured 2026-10-01)

**Repository**
- Public, at `zpconn/aerostore`. No stars, forks, releases or topics.
- Description: "A lock-free, memory-resident Rust database prototype built for high-concurrency market/flight workloads".

**Size**
- 29,179 tracked files, 28,467 of them under `docs/` (about 9 GB).
- The `.git` pack is 6.59 GiB, and more than 99.9% of packed history is `docs/bench_data`.
- The largest campaign, `sustained_capacity_2026-09-28-retry2`, is 5.1 GB.

**History**
- 119 commits on `master`. Evidence was first added in `91d4afce` (2026-03-03). The rewrite changes 57 commits (those that touch evidence, plus their descendants), and the other 62 keep their hashes.
- Authors: `Codex <codex@local>` 62, `Zach Conn` 41, `zpconn` 16.
- The repository-level git config sets `user.name=Codex`.
- Fully merged branches: `diagnose/index-insert-failures-crucible` and `wip/sustained-churn-pressure-fix`.

**Local disk**
- About 326 GiB free of 1007 GiB inside Linux; about 100 GiB free on the Windows volume hosting the WSL disk at Phase 0 admission.
- `target/` is 313 GiB allocated: dozens of campaign directories, plus `verification-tools` at 9.2 GiB.
- Local-only, ignored evidence also lives in `docs/bench_data/sustained_capacity_2026-09-28/`, `docs/bench_data/hyperfeed_socket_write_2026-09-30/evidence/` and `aerostore_core/target/`.

**CI**
- Only `.github/workflows/formal.yml` exists: 38 runs, 34 failed, 4 cancelled, none passed.
- It checks out with `fetch-depth: 0` and builds the frozen set with `rglob`.

**Code**
- `aerostore_core/src` has 26,581 lines in 33 top-level files (27,016 lines in 34 recursive files), with 31 public modules in a flat namespace.
- 3 modules have `//!` docs, and about 5% of public items are documented.
- `cargo fmt --check` fails on 8 files.
- There is no `rust-toolchain.toml`, `.gitattributes`, CHANGELOG, CONTRIBUTING or `.mailmap`.

**Tests and benches**
- 58 test files in core. 12 include bench modules through `#[path]`, and 13 call `fork()` from the test harness.
- The benches total about 22k lines.

**Scripts**
- 46 files, about 15k lines, mostly Python.
- Tests live in both `scripts/test_*.py` and `scripts/tests/`.

**Verification**
- 37 tracked top-level directories, with 24 `generate.py` files plus `ordinary_generate.py`, and 29 `run.py` files.
- 11 TLA+ specs: 10 under `verification/tla/` plus the service protocol spec. The main TLC campaign has 136 cases; the committed logs contain `/home/zpconn`.
- 47 claims (37 partial, 4 in progress, 6 open) and 28 assumptions.
- 24 generated `*.verus.rs` files.

**Docs**
- The README is 289 lines.
- There are 38 `docs/*.md` files, 26 of them `hyperfeed_*`.
- 44 links are broken for anyone but the author.

**Headline result**
- 6,400 vs 704 msg/s (9.09×), documented in `docs/hyperfeed_queue_profile.md`.
- PostgreSQL ran with 128 MiB `shared_buffers`.
- The two campaigns behind the result reference 823 and 3,183 local-only files.

**Existing tools to reuse**
- `scripts/iterate_hyperfeed.py` (paired screens)
- `scripts/hyperfeed_screen_{capture,assessment,resources}.py`
- `qualify_hyperfeed.py`, `assess_hyperfeed_capacity.py`, `hyperfeed_proof_impact.py`
- `verify_formal.py`, `run_verified_experiment.py`, `compare_engine_performance.py`
- `check_lock_models.py`, `setup_verification.py`
- `verification/experiments/profiles/bucket_sets.toml`

## Appendix B: Owner checkpoints

**Phase 0**
- Inventory review and the evidence publication scope.

**Phase 1**
- Task 1.2: merging the gate-changing PR, and the boundary re-baseline.

**Phase 2**
- The dry run, README v1, and the GitHub operations: tag push, rename, new repository and its settings.
- After verification: the `gc` and archiving step.
- Task 2.6: the evidence releases.

**Phase 3**
- Task 3.1: moving `target/verification-tools` to `.tools/`, and any relocation of legacy evidence.

**Phase 4**
- Task 4.1: the proof-tooling re-baseline.
- Pushing the `pre-legacy-removal` tag.

**Phase 5**
- Task 5.7: filing the Aeneas issue.

**Phase 6**
- The protected-path list, CODEOWNERS and branch protection.
- Any self-hosted runner.
- Every promotion.

**Phase 7**
- README v2, including which agents and models are named and the spelling of the name.
- The `.mailmap` display names.
- The GitHub description and topics.

**Phase 8**
- The PostgreSQL tuning choices.
- The v0.1.0 release.
- Zenodo, once D2 is decided.

**At any time**
- Anything destructive.
- Any force-push.
- Any change that touches D2-scoped content.

## Appendix C: Execution log

| Date | Phase/task | Change (PR/commit) | Evidence | Notes |
|---|---|---|---|---|
| 2026-10-01 | — | Plan written | Read-only review | No repository changes yet |
| 2026-10-02 UTC (Oct 1 Chicago) | 0.1 | Local branch `overhaul/phase-0-inventory`; ignored `runs/` | `runs/inventory/2026-10-02/{admission,state,ci-summary}.json`; `latest-ci-failure.log`; CI run https://github.com/zpconn/aerostore/actions/runs/36682049718 | State commands succeeded. Git packs 6.59 GiB; 38 CI runs: 34 failed, four cancelled. Output budget 1 GiB, Linux/Windows reserves 30 GiB each, available-memory reserve 4 GiB. No builds or benchmark trials. |
| 2026-10-02 UTC (Oct 1 Chicago) | 0.3 | Correct Appendix A counts; complete coupling inventory | `runs/inventory/2026-10-02/coupling/{summary.md,coupling.json,appendix_a.json,scan.py,receipt_commit_references.json,receipt_commit_lines.py}` | 67 source-path-coupled files; 49 mentioning `occ_partitioned.rs`. Frozen lock has 24 existing hash mismatches, 20 missing tracked inputs and seven ignored inputs; all 33 P0 module digests match. Receipt scan locates 1,369 revision references, resolving 428 to local commits and identifying 941 as external toolchain pins. No proof boundary refreshed. Formatting/docs coverage estimates were not rerun. |
| 2026-10-02 UTC (Oct 1 Chicago) | 0.2 | Complete evidence inventory and conservative intermediate classification | `runs/inventory/2026-10-02/evidence/{summary.json,campaigns.json,classified_references.jsonl.gz,explicit-local-manifest-audit.json,inventory.sqlite3}`; `candidate-reference-protection.json`; `coupling/directory-protection-review.md` | 109 campaign/root groups; 246,227 file paths; 2,532,668 classified reference-candidate occurrences / 225,191 canonical paths. Main and supplemental hashing read 282,014,215,549 bytes across 126,891 distinct versions. Three historical metadata format defects preserved; oversized diagnostic metadata handled. No pending hashes. Zero confirmed reclaimable bytes; 0.402 GiB intermediate candidates await cleanup-readiness review. No deletion or evidence change. |
| 2026-10-02 UTC (Oct 1 Chicago) | 0.4 | Complete headline gap inventory and proposed publication scope | `runs/inventory/2026-10-02/publication/{PROPOSAL.md,global_archive_closure.json,hash_join.json,transactional_source_resolution.json}` | 5,270 additional local paths / 71,603,390,617 logical bytes beyond tracked manifest representations. Explicit lists 823/823, 3,183/3,183 and 2,507/2,507 plus six tools match. All 23 historical transactional source checksums recovered from Git; historical executable/runtime identities remain unverified. Proposed four mutable packages, 8 GiB staging cap, separate later publication/content approvals. |
| 2026-10-02 UTC (Oct 1 Chicago) | Checkpoint 0 (awaiting owner review) | Inventory ready for review; no Phase 1 work | `runs/inventory/2026-10-02/SUMMARY.md`; `review-checks.json` | All inventory/hash/export jobs completed. Inventory totals, recovered-source counts and local review links checked. Approximately 0.85 GiB of new inventory output, within the 1 GiB admission budget. Existing source, workload, judges and evidence unchanged. No external mutation, commit, push, cleanup or publication. |
| 2026-10-02 UTC (Oct 1 Chicago) | Checkpoint 0 approved; Phase 1 started | Owner replied “approved” in the current conversation | `runs/inventory/2026-10-02/SUMMARY.md`; `runs/overhaul-phase1/2026-10-02/admission.json` | Inventory/publication planning scope accepted. Phase 1 CI/gate implementation authorized; later merge, publication, history and settings checkpoints remain separate. Local additional-output budget 2 GiB; 30 GiB guest/host reserves. Required Rust/proof builds will use fresh hosted CI to preserve recorded executables in the normal local Cargo target. |
