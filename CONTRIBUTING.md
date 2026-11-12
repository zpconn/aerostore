# Contributing to Aerostore

Thanks for your interest. Aerostore is an experimental engine, and its value
depends on its claims being true. The rules below protect that. They apply
equally to human and AI contributors.

## What helps most

- **Bug reports with a reproducer.** Include the failing test or command, the
  commit, the host (CPU, kernel, WSL2 or native Linux) and the full output.
- **Workloads.** A reproducible workload that exposes a correctness or
  performance problem is worth more than a speedup claim.
- **Focused pull requests.** Change one thing per PR, and say what evidence
  supports it.

Open an issue before starting a large change, especially one that touches the
transaction protocol, on-disk or shared-memory formats, or the verification
boundary.

## Set up

Aerostore targets Linux, including WSL2, with Rust **1.93.1**. On Debian or
Ubuntu:

```sh
sudo apt-get install -y build-essential pkg-config tcl tcl-dev clang libclang-dev python3
cargo build --workspace --locked
```

Builds, verification campaigns and benchmarks can use a lot of disk. Read the
[disk-space runbook](docs/disk-space.md) before running anything substantial.

## Checks to run

These are the checks CI runs on every pull request:

```sh
python3 -I scripts/check_formal_coverage.py   # proof-input lock is consistent
python3 tools/evidence/check_catalog.py        # evidence catalog and retained copies
python3 tools/check_links.py                   # Markdown links (checks the Git index; stage new files first)
python3 scripts/run_python_unit_tests.py       # Python tooling tests
cargo build --workspace --all-targets --locked
cargo test --workspace --lib --locked
cargo test -p aerostore_macros --locked
cargo doc --workspace --no-deps --locked
```

`cargo fmt --check` and `cargo clippy -- -D warnings` currently report without
blocking, while a formatting baseline is reviewed. Don't add new warnings.

Many integration tests fork processes and share mappings. Run them serially:

```sh
cargo test --workspace --release --locked -- --test-threads=1
```

## Changes that touch proofs

Part of the engine is bound to proofs by a committed proof-input lock,
[`verification/frozen_boundary.json`](verification/frozen_boundary.json). If a
change touches engine source, macros, toolchain pins or verification inputs:

1. Stage the intended source changes.
2. Regenerate and inspect the lock:
   ```sh
   python3 -I scripts/check_formal_coverage.py --write-boundary
   python3 -I scripts/check_formal_coverage.py --review-base master
   git diff -- verification/frozen_boundary.json
   ```
3. Expect the `Verify` workflow to run the component pilot on the PR.

A changed lock is a request for review, not an approval. See
[CI and proof-input review](docs/ci.md) and the
[verification workspace](verification/README.md).

## Never weaken a judge

Tests, specifications, oracles, thresholds and negative controls are the
**judges** that decide whether a change is acceptable. They are not obstacles
to route around.

- Do not delete or loosen a judge to make a change pass.
- If a judge is wrong, fix it in a **separate** PR that says so and explains
  why. The owner reviews every such PR.
- `CODEOWNERS` marks the protected paths: tests, benchmarks, verification,
  scripts, workflows and build configuration.

## Performance claims

A performance change is accepted on evidence, not on a single good run:

1. **Focused checks.** Run the relevant unit tests, proofs and a component
   benchmark.
2. **Paired screens.** Compare the preserved baseline and candidate
   executables in alternating order with
   [`scripts/iterate_hyperfeed.py`](docs/hyperfeed_iteration.md).
3. **Sustained qualification** for finalists only: repeated long trials plus a
   matching full-history correctness companion.

Keep correctness and allocation checks enabled while measuring. Report failed
and inconclusive results alongside successes. A rejected candidate is a
useful result.

## Evidence

Bulk campaign evidence does not go in Git. The [evidence catalog](evidence/README.md)
describes where it lives and how hashes are checked. Small summaries that a
report depends on may be committed under `evidence/`. Never edit, move or delete
receipts, manifests, logs or recorded executables, including failed runs and
negative controls.

## Writing style

Reports and docs follow one house style.
[`docs/hyperfeed_queue_profile.md`](docs/hyperfeed_queue_profile.md) and
[`docs/worker_failure_contract.md`](docs/worker_failure_contract.md) are good
models.

- **Conclusion first.** The first paragraph says what was found.
- **One caveats block per result**, placed next to the result.
- **Numbers go in tables**, at sensible precision: 9.0 ms, not 9.014991 ms.
- **Present tense for what is true now.** History belongs in dated reports.
- **Reports are records.** A new finding gets a new report that supersedes the
  old one. Don't rewrite the old one's conclusions.

## AI agents

Coding agents are welcome here; Aerostore itself was built with them.
Agents follow [AGENTS.md](AGENTS.md). Agent commits keep the human owner as
author and add a `Co-Authored-By:` trailer that names the agent and model, so
the history stays honest about who did what.

## License

By contributing, you agree that your contributions are licensed under the
[MIT License](LICENSE).
