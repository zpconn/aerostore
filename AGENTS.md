# Repository instructions

## Disk usage

Follow [the disk-space runbook](docs/disk-space.md) when building, running
verification or benchmarks, or cleaning generated files.

- Before a substantial build or campaign, measure allocated usage and free space.
  Under WSL, also check the Windows volume hosting the VHDX. Budget for concurrent
  builds, all retained variants, histories, and a stated free-space reserve.
  Recheck between batches; reduce concurrency or reclaim known intermediates
  before starting work that would exceed that budget.
- Reuse the normal Cargo target for ordinary development. Verification source
  snapshots and mutants must keep the fresh, isolated targets their runners
  require. Never share those targets or rebuild inside retained evidence.
- After a completed campaign, including a failed or interrupted one whose
  processes have exited, remove disposable compiler intermediates promptly.
  Coordinate with other agents and jobs; never prune an active build directory.
  Do not leave a full dependency build for every historical variant.
- Preserve source snapshots, receipts, manifests, logs, benchmark histories,
  diagnostic scripts, recorded executables, and their runtime dependencies.
  Keep referenced paths and file contents unchanged, including negative controls
  and failure evidence. Never strip or overwrite binaries to save space.
- Before deleting compiled artifacts, identify receipt references and runtime
  dependencies. Record existing missing files, verify retained hashes before and
  after cleanup, and report newly missing or changed artifacts as failures.
- Treat `target/verification-tools` and the Lean dependency installation as
  retained tools. `target/` also contains evidence and runnable deliverables:
  do not use blanket `cargo clean`, `rm -rf target`, or `git clean -fdx` here.
  Limit cleanup to identified compiler intermediates in the owning build tree.
- Report space reclaimed and evidence checks after cleanup. Under WSL, distinguish
  free space inside Linux from space returned to Windows; do not shut down WSL or
  manipulate an attached VHDX as part of routine project cleanup.
