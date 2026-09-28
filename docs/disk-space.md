# Managing disk space

Verification campaigns retain independent source snapshots and builds for many
variants. Without cleanup, each campaign leaves another set of compiler caches.
On September 28, 2026, `target/` occupied about 479 GiB; 416 `cargo-target`
directories accounted for about 355 GiB. Removing audited intermediates reduced
`target/` to about 90 GiB while retaining histories and evidence executables.
These measurements describe that cleanup, not a permanent storage budget.

## Before and during a campaign

From the repository root, inspect allocated usage and filesystem capacity:

```sh
df -h .
du -xhd1 target | sort -h
```

Under WSL, also run `df -h /mnt/c` when the VHDX lives on C:. Use the actual host
volume if it lives elsewhere. Linux can report ample free capacity while the
Windows volume is nearly full. `/tmp` may use the same disk and is not a way to
avoid the storage budget.

Record the expected additional storage and free-space reserve before launching
a large campaign. Estimate from a comparable run or a small representative
batch. Include dependency builds, source copies, parallel workers, variants
retained until campaign completion, and growing histories. Budget from the
smaller usable headroom of the guest and host filesystems when both apply.
Account for hard links: summing separate `du` results can overstate reclaimable
space. Recheck between batches and before increasing workload size or concurrency.

If the next batch would consume the reserve, reduce the batch size or concurrency
and clean completed build intermediates before continuing. Do not start another
full campaign merely to replace a retryable step when the runner supports a
valid, bounded retry. Do not weaken freshness or verification requirements to
reuse old results.

Routine development can reuse the ordinary Cargo target. Verification variants
must retain separate targets: [the lock-model runner](../scripts/check_lock_models.py)
explicitly forbids sharing targets between source roots, and the
[planning runner](../verification/planning_native/run.py) requires a fresh output
directory. Shared targets can reuse artifacts from the wrong source snapshot.
Clean intermediate files after the campaign has finished and its evidence has
been validated; do not change the build isolation to save space.

## What to retain

Being under ignored `target/` does not make a file disposable.

| Retain | Reason |
| --- | --- |
| Receipts, manifests, source snapshots, patches, logs, traces, and diagnostic scripts | Describe the inputs, execution, failures, and scope of a result |
| Full benchmark histories such as `history.jsonl` | May be inputs to correctness checks and later analysis |
| Executables named by receipts and their runtime dependencies | Evidence checkers validate their original paths and hashes |
| Other test executables unless established to be disposable | Older campaigns may record them only in logs |
| Runnable deliverables, including `libaerostore_tcl.so` | Used directly by the project and examples |
| `target/verification-tools`, pinned toolchains, and `verification/lean/.lake` | Required verification tools and dependencies |

Failed, interrupted, and intentionally failing runs can contain necessary
diagnostic evidence. Keep those records even when pruning their compiler caches.
Do not move, strip, overwrite, or compress referenced files in place. Do not edit
receipt hashes to make a cleanup appear valid. Archiving or removing retained
evidence requires a separate retention decision and checked downstream references.

## Cleaning compiler intermediates

1. Confirm that the owning build, tests, and benchmark processes have exited.
   Coordinate with other agents and jobs that use the same checkout. Record the
   current Git status and disk usage.
2. Identify the exact Cargo build roots. Enumerate artifacts referenced by the
   campaign receipts, manifests, and logs, and record hashes and any files already
   missing. Reference schemas include native `checks[].binary_sha256`, lock-model
   `builds[].executable` with `executable_sha256`, and performance manifest
   `binaries.*.path` with `sha256`. These examples are not an exhaustive keep list.
3. Preserve those artifacts and their runtime dependencies. Inspect ELF dynamic
   dependencies with `readelf -d` when considering removal of local libraries or
   build outputs. The test binaries audited in September 2026 used only system
   libraries; future binaries may require local `.so` files or runtime resources.
4. Remove only confirmed intermediates in those build roots: incremental caches,
   Cargo fingerprints, dependency metadata, `.rlib`, `.rmeta`, and object files.
   Inspect `build/` before pruning it because generated libraries or resources may
   be needed at runtime. Remove procedural-macro libraries only after confirming
   that retained executables and tools do not depend on them. Preserve unknown
   files and symlinks rather than following them outside the selected build tree.
5. Verify retained artifact paths and hashes against the pre-cleanup inventory and
   receipts. Run the applicable evidence validators for current accepted results.
   Distinguish preexisting missing or stale evidence from any cleanup regression;
   do not rewrite historical records. Confirm source files and Git status are
   unchanged by cleanup, and measure actual filesystem space reclaimed.

Do this after each substantial campaign, so historical variants retain evidence
instead of complete dependency builds. Handle failed or interrupted campaigns
once their processes have exited and diagnostic records are secured. Removing
intermediates makes the next build slower; it should not change the source,
recorded evidence, or required build settings. Do not run broad rebuilds just to
validate cache removal, since they immediately recreate the discarded data.

Never clean all of `target/` or run repository-wide `cargo clean` or
`git clean -fdx`: they can remove evidence and installed verification tools.

## Returning space to Windows

Deleting files releases Linux filesystem space. It may leave the Windows VHDX
allocation unchanged. Report both measurements instead of claiming that Linux
cleanup has reclaimed host space.

Trimming free blocks and, if needed, offline VHDX compaction are separate WSL
maintenance steps. Schedule shutdown outside active project work and use the
actual distribution and disk path. Microsoft's
[compaction documentation](https://learn.microsoft.com/en-us/windows-server/administration/windows-commands/compact-vdisk)
requires the disk to be detached or attached read-only. Do not manipulate a live
writable VHDX. The `sparseVhd` setting applies automatically to newly created
disks; it does not establish that an existing disk is sparse. See
[WSL configuration](https://learn.microsoft.com/en-us/windows/wsl/wsl-config).
