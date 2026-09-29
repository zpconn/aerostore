#!/usr/bin/env python3
"""Disk/memory admissions for short HyperFeed screens; never removes artifacts.

The baseline is written once under the campaign output. Resume with that same
receipt, not a new preflight. All target and Git growth since admission counts,
including other concurrent work; this deliberately errs toward stopping a batch.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import subprocess

GIB = 1024 ** 3
MAX_BUDGET_BYTES = 20 * GIB
DISK_RESERVE_BYTES = 30 * GIB
MEMORY_RESERVE_BYTES = 4 * GIB
FORMAT = "hyperfeed-screen-resource-baseline-v1"


def is_wsl():
    return any("microsoft" in path.read_text().lower()
               for path in (Path("/proc/sys/kernel/osrelease"), Path("/proc/version"))
               if path.exists())


def allocated_bytes(path):
    """Allocated blocks, not apparent file lengths; do not follow child symlinks."""
    result = subprocess.run(["du", "--summarize", "--one-file-system",
                             "--block-size=1", "--", str(path)],
                            check=True, text=True, capture_output=True)
    return int(result.stdout.split()[0])


def mem_available_bytes():
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    raise RuntimeError("MemAvailable is missing from /proc/meminfo")


def _directory(path):
    result = Path(path).resolve(strict=True)
    if not result.is_dir():
        raise ValueError(f"Not a directory: {result}")
    return result


def _host_volume(host_volume):
    wsl = is_wsl()
    selected = host_volume if host_volume is not None else ("/mnt/c" if wsl else None)
    if selected is None:
        return None, wsl
    path = _directory(selected)
    if not path.is_mount():
        raise ValueError(f"Host volume must be an actual mounted filesystem: {path}")
    return path, wsl


def resource_snapshot(target, *, guest_path=None, host_volume=None, git_path=None):
    """Measure Linux and declared VHDX-host volume; /mnt/c is the WSL default.

    Callers must override host_volume when the distribution's VHDX is elsewhere.
    Absence of a mount or a failed measurement is an error, never a free-space
    estimate. Linux outside WSL needs no host-volume measurement.
    """
    target = _directory(target)
    guest = _directory(guest_path if guest_path is not None else target)
    if guest.stat().st_dev != target.stat().st_dev:
        raise ValueError("Campaign output and target must share the guest filesystem")
    host, wsl = _host_volume(host_volume)
    if git_path is None:
        candidate = target.parent / ".git"
        git_path = candidate if candidate.is_dir() else None
        if candidate.is_file():
            raise ValueError("Git worktree indirection requires explicit git_path")
    git = _directory(git_path) if git_path is not None else None
    if git is not None:
        if git == target or git.is_relative_to(target) or target.is_relative_to(git):
            raise ValueError("Git and target allocation roots must not overlap")
        if git.stat().st_dev != target.stat().st_dev:
            raise ValueError("Git allocation must share the measured guest filesystem")
    return {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "target_path": str(target), "target_allocated_bytes": allocated_bytes(target),
        "git_path": str(git) if git else None,
        "git_allocated_bytes": allocated_bytes(git) if git else 0,
        "guest_path": str(guest), "guest_free_bytes": shutil.disk_usage(guest).free,
        "is_wsl": wsl, "host_volume": str(host) if host else None,
        "host_free_bytes": shutil.disk_usage(host).free if host else None,
        "host_volume_contract": "Caller declares actual VHDX host volume; WSL default is /mnt/c",
        "mem_available_bytes": mem_available_bytes(),
    }


def _positive_integer(value, label, *, allow_zero=False):
    if isinstance(value, bool) or not isinstance(value, int) or value < (0 if allow_zero else 1):
        raise ValueError(f"{label} must be {'nonnegative' if allow_zero else 'positive'} integer bytes")


def _decision(baseline, snapshot, prospective_bytes):
    _positive_integer(prospective_bytes, "prospective_bytes", allow_zero=True)
    total = baseline["total_budget_bytes"]
    _positive_integer(total, "total_budget_bytes")
    if total > MAX_BUDGET_BYTES:
        raise ValueError("Screen campaign budget cannot exceed 20 GiB")
    initial = baseline["initial_snapshot"]
    for key in ("target_path", "git_path", "guest_path", "host_volume", "is_wsl"):
        if snapshot[key] != initial[key]:
            raise ValueError(f"Resource identity changed since baseline: {key}")
    growth = {name: max(0, snapshot[name] - initial[name])
              for name in ("target_allocated_bytes", "git_allocated_bytes")}
    allocated_growth = sum(growth.values())
    forecast = allocated_growth + prospective_bytes
    reasons = []
    if forecast > total:
        reasons.append("allocated growth plus prospective batch exceeds original campaign budget")
    if snapshot["guest_free_bytes"] < DISK_RESERVE_BYTES + prospective_bytes:
        reasons.append("guest free space would breach 30 GiB reserve")
    if snapshot["host_volume"] is not None and snapshot["host_free_bytes"] < DISK_RESERVE_BYTES + prospective_bytes:
        reasons.append("declared VHDX host volume would breach 30 GiB reserve")
    if snapshot["mem_available_bytes"] < MEMORY_RESERVE_BYTES:
        reasons.append("MemAvailable is below 4 GiB reserve")
    return {"passed": not reasons, "reasons": reasons, "snapshot": snapshot,
            "positive_growth_by_root": growth, "allocated_growth_bytes": allocated_growth,
            "prospective_bytes": prospective_bytes, "forecast_growth_bytes": forecast,
            "total_budget_bytes": total, "remaining_budget_bytes": total - forecast}


def preflight(target, output, *, prospective_bytes=8 * GIB,
              total_budget_bytes=MAX_BUDGET_BYTES, host_volume=None, git_path=None):
    """Create output/resource-baseline.json exactly once, including failed admissions.

    Budget covers normal build caches, screens, retained evidence and Git growth.
    Reserve prospective_bytes for the entire next batch, including an optional
    build; use check_budget with this immutable receipt before every later batch.
    """
    _positive_integer(total_budget_bytes, "total_budget_bytes")
    _positive_integer(prospective_bytes, "prospective_bytes", allow_zero=True)
    if total_budget_bytes > MAX_BUDGET_BYTES:
        raise ValueError("Screen campaign budget cannot exceed 20 GiB")
    target = _directory(target)
    output = Path(output).resolve()
    if output == target or not output.is_relative_to(target):
        raise ValueError("Screen output must be a directory beneath target")
    baseline_path = output / "resource-baseline.json"
    if baseline_path.exists():
        raise FileExistsError(f"Resume with existing resource baseline: {baseline_path}")
    output.mkdir(parents=True, exist_ok=True)
    snapshot = resource_snapshot(target, guest_path=output, host_volume=host_volume, git_path=git_path)
    baseline = {"format": FORMAT, "baseline_path": str(baseline_path),
                "total_budget_bytes": total_budget_bytes,
                "disk_reserve_bytes": DISK_RESERVE_BYTES,
                "memory_reserve_bytes": MEMORY_RESERVE_BYTES,
                "initial_snapshot": snapshot}
    baseline["admission"] = _decision(baseline, snapshot, prospective_bytes)
    baseline["passed"] = baseline["admission"]["passed"]
    baseline["reasons"] = baseline["admission"]["reasons"]
    with baseline_path.open("x") as stream:
        json.dump(baseline, stream, indent=2, sort_keys=True)
        stream.write("\n")
    return baseline


def check_budget(baseline, *, prospective_bytes):
    """Recheck against the original baseline without updating it or deleting files."""
    if not isinstance(baseline, dict):
        baseline = json.loads(Path(baseline).read_text())
    if baseline.get("format") != FORMAT:
        raise ValueError("Unsupported screen resource baseline")
    if baseline.get("disk_reserve_bytes") != DISK_RESERVE_BYTES or baseline.get("memory_reserve_bytes") != MEMORY_RESERVE_BYTES:
        raise ValueError("Original disk/memory reserve must not change")
    initial = baseline["initial_snapshot"]
    snapshot = resource_snapshot(initial["target_path"], guest_path=initial["guest_path"],
                                 host_volume=initial["host_volume"], git_path=initial["git_path"])
    return _decision(baseline, snapshot, prospective_bytes)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    first = commands.add_parser("preflight")
    first.add_argument("--target", type=Path, required=True)
    first.add_argument("--output", type=Path, required=True)
    first.add_argument("--host-volume", type=Path)
    first.add_argument("--git-path", type=Path)
    first.add_argument("--total-budget-bytes", type=int, default=MAX_BUDGET_BYTES)
    first.add_argument("--prospective-bytes", type=int, default=8 * GIB)
    again = commands.add_parser("check")
    again.add_argument("--baseline", type=Path, required=True)
    again.add_argument("--prospective-bytes", type=int, required=True)
    args = vars(parser.parse_args())
    command = args.pop("command")
    result = preflight(**args) if command == "preflight" else check_budget(**args)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
