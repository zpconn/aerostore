#!/usr/bin/env python3
"""Collect exploratory HyperFeed capacity evidence without promoting an engine.

Use a built hyperfeed_contention_crucible binary and an explicitly managed local
PostgreSQL server. Credentials are read only from --pg-url-env. Each matrix cell
is a fresh benchmark process; every seed, failure, complete report and log is
retained. Source hashes include dirty Rust, workload, proof and gate inputs.

--workload lifecycle preserves the concentrated generation stress corpus.
--workload fleet requires --hot-percent 0 and at least 16 configured identities;
it starts from 16 warmup rounds and interleaves live generations across identities.
--workload calibrated separates ordered foreground traffic from projection and
housekeeping timers. Optional signature affinity can reorder one flight across
workers; the history oracle checks that order, while discarded stale updates
remain excluded from complete useful-work evidence. Short/accelerated checks can validate execution, but cannot
demonstrate the operator's five-to-ten-minute maintenance cadence. Its fixed
population remains capacity-unqualified even when --maintenance-mode sweep
covers complete batched sweeps at representative intervals.

First use --evidence full to produce a correctness companion. A metrics campaign
may name that campaign's campaign.json via --correctness-report. Companions must
have identical source/binary fingerprints and exact engine/configuration/corpus
coverage. A companion does NOT verify the metrics run's unrecorded history.

Public helpers assess_trial(), build_gate(), snapshot_sources(), and run_process()
are deliberately testable without databases. Tested capacities are lower bounds;
their ratio alone cannot establish a 10x claim. Even a synthetic capacity bound
does not qualify real HyperFeed compatibility, recovery, or MMHF availability.
"""
from __future__ import annotations

import argparse
from functools import lru_cache
from datetime import datetime, timezone
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import platform
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from urllib.parse import urlsplit, unquote

ROOT = Path(__file__).resolve().parents[1]
FORMAT = "hyperfeed-qualification-v1"
ENGINES = ("aerostore", "service-unix", "service-tcp", "postgres")
CORPUS_FIELDS = ("workload", "families", "hot_percent", "seed", "arrival_rate", "seconds")
MATCH_FIELDS = CORPUS_FIELDS + ("engine", "workers", "pg_write_mode", "rpc_delay_us", "global_time_predicates", "shm_mib", "max_backlog", "max_messages", "message_interval_us")
CALIBRATED_FIELDS = ("projection_interval_seconds", "housekeeping_interval_seconds")
DISPATCH_DEFAULTS = {"dispatch": "identity", "affinity_ttl_ms": 0, "signature_pattern": "both"}
MAINTENANCE_DEFAULTS = {"maintenance_mode": "batch", "projection_batch_size": 4,
                        "housekeeping_batch_size": 32, "max_maintenance_batches": 4096}
CALIBRATED_DEFAULTS = {**DISPATCH_DEFAULTS, **MAINTENANCE_DEFAULTS}
EXPERIMENT_DEFAULTS = {"expiry_index_policy": "all-active", "retry_diagnostics": False}
EFFECT_FIELDS = ("created_views", "updated_views", "outputs", "claimed_events", "cancelled_events", "rescheduled_events", "expired_records", "expired_families")
OUTCOME_FIELDS = EFFECT_FIELDS + ("missing_family", "allocation_deferred", "ignored_stale", "duplicate_messages")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def snapshot_sources(root: Path = ROOT) -> dict:
    """Hash working bytes, including untracked source; never substitute HEAD."""
    files = {}
    for name in ("Cargo.toml", "Cargo.lock", "rust-toolchain", "rust-toolchain.toml"):
        path = root / name
        if path.is_file():
            files[name] = sha256(path)
    for name in ("aerostore_core", "aerostore_verified", "aerostore_macros", "aerostore_tcl", "verification", "scripts", ".cargo"):
        directory = root / name
        if not directory.exists():
            continue
        for current, dirs, names in os.walk(directory):
            dirs[:] = sorted(d for d in dirs if d not in {"target", ".git", ".lake", "__pycache__", ".venv"})
            for filename in sorted(names):
                path = Path(current) / filename
                if path.suffix in {".rs", ".toml", ".lock", ".lean", ".tla", ".cfg", ".json", ".py", ".sh"}:
                    files[str(path.relative_to(root))] = sha256(path)
    encoded = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
    return {"sha256": hashlib.sha256(encoded).hexdigest(), "files": files}


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")
    temporary.replace(path)


def finite_number(value) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def counts_match(actual, expected: dict) -> bool:
    return isinstance(actual, dict) and actual == expected and all(type(value) is int for value in actual.values())


def match_fields(config: dict) -> tuple:
    # Preserve historical stress keys; irrelevant timer defaults do not change
    # the fixed lifecycle/fleet corpora or their existing assessments.
    return MATCH_FIELDS + tuple(EXPERIMENT_DEFAULTS) + (CALIBRATED_FIELDS + tuple(CALIBRATED_DEFAULTS) if config.get("workload") == "calibrated" else ())


def config_value(config: dict, field: str):
    return config.get(field, {**CALIBRATED_DEFAULTS, **EXPERIMENT_DEFAULTS}.get(field))


def key(config: dict) -> tuple:
    return tuple(config_value(config, field) for field in match_fields(config))


def dispatch_config(config: dict) -> tuple[str, int, str]:
    dispatch, ttl, pattern = (config_value(config, field) for field in DISPATCH_DEFAULTS)
    if (dispatch not in {"identity", "signature-affinity"} or pattern not in {"both", "mixed"}
            or type(ttl) is not int or not 0 <= ttl <= 3600000
            or (dispatch == "signature-affinity") != (ttl > 0)):
        raise ValueError("signature-affinity requires an explicit TTL in 1..3600000 ms; identity requires TTL 0")
    return dispatch, ttl, pattern


def maintenance_config(config: dict) -> tuple[str, int, int, int]:
    mode, projection, housekeeping, cap = (config_value(config, field) for field in MAINTENANCE_DEFAULTS)
    if (mode not in {"batch", "sweep"} or type(projection) is not int or not 1 <= projection <= 16
            or type(housekeeping) is not int or not 1 <= housekeeping <= 64
            or type(cap) is not int or not 1 <= cap <= 4096):
        raise ValueError("maintenance needs batch|sweep, projection batch 1..16, housekeeping batch 1..64 and batch cap 1..4096 including terminal")
    return mode, projection, housekeeping, cap


@lru_cache(maxsize=64)
def _dispatch_summary(active: int, workers: int, foreground: int, rate: int,
                      dispatch: str, ttl: int, pattern: str) -> dict:
    """Reconstruct routing from offered inputs, without Rust assignments.

    The finite fixture retains expired signatures so new and expired misses
    remain distinguishable. Neither database results nor completions select an
    owner. The fingerprint encodes every global input index and assigned owner.
    """
    cache, seen, last_owner, owners = {}, set(), {}, {}
    counters = dict(hits=0, misses=0, new_signature_misses=0, expired_misses=0,
                    expired_owner_changes=0, planned_flight_worker_changes=0)
    counts, cursor, fingerprint = [0] * workers, 0, 0xcbf29ce484222325
    for sequence in range(foreground):
        identity, ordinal = sequence % active, sequence // active
        alias = (ordinal // 2) % 3 if pattern == "mixed" else 0
        signature = (0 if alias == 2 else 100 + identity // 4,
                     0 if alias == 1 else 10000 + identity)
        seen.add(signature)
        if dispatch == "identity":
            owner = identity % workers
        else:
            arrival = sequence * 1000000000 // rate
            previous = cache.get(signature)
            if previous is not None and arrival < previous[1]:
                owner = previous[0]
                counters["hits"] += 1
            else:
                owner, cursor = cursor, (cursor + 1) % workers
                counters["misses"] += 1
                counters["new_signature_misses" if previous is None else "expired_misses"] += 1
                counters["expired_owner_changes"] += int(previous is not None and previous[0] != owner)
            cache[signature] = (owner, arrival + ttl * 1000000)
        counts[owner] += 1
        counters["planned_flight_worker_changes"] += int(identity in last_owner and last_owner[identity] != owner)
        last_owner[identity] = owner
        owners.setdefault(identity, set()).add(owner)
        for byte in sequence.to_bytes(8, "little") + owner.to_bytes(8, "little"):
            fingerprint = ((fingerprint ^ byte) * 0x100000001b3) & 0xffffffffffffffff
    return {"policy_version": "calibrated-dispatch-v1", "dispatch": dispatch,
            "affinity_ttl_ms": ttl, "signature_pattern": pattern,
            "clock": "scheduled_arrival_offset", **counters,
            "flights_with_multiple_workers": sum(len(value) > 1 for value in owners.values()),
            "unique_signatures": len(seen), "worker_counts": counts,
            "assignment_fingerprint_format": "fnv1a64-q-u64le-owner-u64le",
            "assignment_fingerprint": f"{fingerprint:016x}"}


def calibrated_dispatch(config: dict) -> dict:
    dispatch, ttl, pattern = dispatch_config(config)
    families = config["families"]
    return _dispatch_summary(families - max(1, families // 4), config["workers"],
                             config["arrival_rate"] * config["seconds"], config["arrival_rate"],
                             dispatch, ttl, pattern)


def calibrated_timer_counts(config: dict) -> tuple[int, int]:
    seconds = config.get("seconds")
    intervals = [config.get(field) for field in CALIBRATED_FIELDS]
    if (type(seconds) is not int or not 1 <= seconds <= 3600
            or any(type(value) is not int or not 1 <= value <= 3600 for value in intervals)):
        raise ValueError("calibrated duration and timer intervals must be integer seconds in 1..3600")
    # The first tick follows one complete interval; a tick at the exclusive
    # admission endpoint is not in the offered corpus.
    return tuple((seconds - 1) // interval for interval in intervals)


def calibrated_corpus(config: dict) -> dict:
    """Independent arithmetic for foreground identity lanes and timer jobs."""
    families, workers, rate = (config.get(name) for name in ("families", "workers", "arrival_rate"))
    if (type(families) is not int or not 4 <= families <= 1024 or config.get("hot_percent") != 0
            or type(workers) is not int or not 1 <= workers <= 32
            or type(rate) is not int or not 1 <= rate <= 1000000):
        raise ValueError("calibrated needs 4..1024 uniform identities and bounded positive foreground workers/rate")
    quiet = max(1, families // 4)
    active = families - quiet
    dispatch, _, _ = dispatch_config(config)
    maintenance_config(config)
    projection, housekeeping = calibrated_timer_counts(config)
    foreground = rate * config["seconds"]
    rounds, remainder = divmod(foreground, active)
    worker_counts = [0] * workers
    for identity in range(active):
        worker_counts[identity % workers] += rounds + int(identity < remainder)
    if dispatch == "signature-affinity":
        worker_counts = calibrated_dispatch(config)["worker_counts"]
    plans = (foreground // (16 * active)) * active + min(foreground % (16 * active), active)
    kinds = {"plan": plans, "position": foreground - plans,
             "global_projection": projection, "global_housekeeping": housekeeping}
    return {"foreground": foreground, "projection": projection, "housekeeping": housekeeping,
            "total": foreground + projection + housekeeping,
            "active_families": active, "quiet_families": quiet,
            "worker_counts": worker_counts + [projection, housekeeping],
            "kinds": {name: count for name, count in kinds.items() if count}}


def maintenance_report_errors(run: dict, config: dict) -> list[str]:
    """Reconcile job evidence without pretending to recheck its SQL history.

    The Rust oracle validates complete terminal queries in full evidence mode.
    These independent checks bind jobs to admission, batch bounds and reported
    effects, and stop transaction throughput from masquerading as message work.
    """
    mode, projection_limit, housekeeping_limit, cap = maintenance_config(config)
    corpus = calibrated_corpus(config)
    kinds = corpus["kinds"]
    per_kind = run.get("per_kind")
    if not isinstance(per_kind, dict) or any(not isinstance(value, dict) or not isinstance(value.get("outcomes"), dict)
                                           for value in per_kind.values()):
        return ["maintenance transaction accounting requires per-kind objects"]
    if mode == "batch":
        expected = corpus["total"]
        if (type(run.get("completed_transactions", expected)) is not int or run.get("completed_transactions", expected) != expected
                or not counts_match(run.get("transaction_kinds", kinds), kinds)
                or any(type(value.get("transactions", value.get("completed"))) is not int
                       or value.get("transactions", value.get("completed")) != value.get("completed")
                       for value in per_kind.values())):
            return ["batch control transaction counts differ from its one-transaction jobs"]
        return []
    jobs = run.get("maintenance_jobs")
    expected_jobs = corpus["projection"] + corpus["housekeeping"]
    if not isinstance(jobs, list) or len(jobs) != expected_jobs:
        return ["maintenance sweep jobs are missing, duplicated or inflated"]
    admission, workload_end = run.get("admission_started_ns"), run.get("workload_completed_ns")
    if any(type(value) is not int or not 0 <= value < 2**64 for value in (admission, workload_end)):
        return ["maintenance sweep requires valid continuous admission/completion timestamps"]
    totals = dict(committed_batches=0, nonempty_batches=0, terminal_batches=0, processed_rows=0)
    class_jobs = {"projection": [], "housekeeping": []}
    seen = set()
    errors = []
    for job in jobs:
        if not isinstance(job, dict):
            errors.append("maintenance sweep job is not an object")
            continue
        integer_fields = ("worker", "job_id", "job_ordinal", "scheduled_ns", "started_ns", "finished_ns", "received_ns",
                          "batches", "nonempty_batches", "terminal_batches", "processed_rows", "retries",
                          "first_transaction_id", "terminal_transaction_id")
        if any(type(job.get(field)) is not int or not 0 <= job[field] < 2**64 for field in integer_fields):
            errors.append("maintenance sweep job has missing or invalid integer metadata")
            continue
        name, ordinal = job.get("class"), job["job_ordinal"]
        if name not in class_jobs:
            errors.append("maintenance sweep has an unexpected class")
            continue
        bit = int(name == "housekeeping")
        identity = (name, ordinal)
        expected_id = 4000000000 + ordinal * 2 + bit
        first_id = 8000000000 + (expected_id - 4000000000) * 4096
        if (identity in seen or ordinal >= corpus[name] or job["worker"] != config["workers"] + bit
                or job["job_id"] != expected_id
                or job["scheduled_ns"] != admission + (ordinal + 1) * config[name + "_interval_seconds"] * 1000000000
                or job["first_transaction_id"] != first_id
                or job["terminal_transaction_id"] != first_id + job["batches"] - 1):
            errors.append("maintenance sweep job identity, admission or transaction IDs differ from offered work")
        seen.add(identity)
        if not job["scheduled_ns"] <= job["started_ns"] < job["finished_ns"] <= job["received_ns"] <= workload_end:
            errors.append("maintenance sweep job timestamps are not a complete ordered interval")
        limit = housekeeping_limit if bit else projection_limit
        if (not 1 <= job["batches"] <= cap or job["terminal_batches"] != 1
                or job.get("terminal_empty") is not True or job["batches"] != job["nonempty_batches"] + 1
                or not job["nonempty_batches"] <= job["processed_rows"] <= job["nonempty_batches"] * limit):
            errors.append("maintenance sweep omitted its empty terminal, exceeded a cap, or misstated processed batches")
        outcomes = job.get("outcomes")
        if (not isinstance(outcomes, dict) or any(type(outcomes.get(field)) is not int or outcomes[field] < 0 for field in OUTCOME_FIELDS)
                or outcomes.get("expired_records" if bit else "claimed_events") != job["processed_rows"]
                or (job["processed_rows"] == 0 and any(outcomes.get(field) for field in OUTCOME_FIELDS))):
            errors.append("maintenance sweep effects differ from processed rows or invent empty-job effects")
        class_jobs[name].append(job)
        for field, source in (("committed_batches", "batches"), ("nonempty_batches", "nonempty_batches"),
                              ("terminal_batches", "terminal_batches"), ("processed_rows", "processed_rows")):
            totals[field] += job[source]
    if errors:
        return errors
    audit = run.get("maintenance_job_audit", {})
    required = {"checked": True, "passed": True, "completed_jobs": expected_jobs, **totals,
                "scope": "complete_sweep_batched_transactions"}
    if not isinstance(audit, dict) or any(type(audit.get(field)) is not type(value) or audit[field] != value for field, value in required.items()):
        errors.append("maintenance sweep summary differs from individual completed jobs")
    transaction_kinds = {kind: count for kind, count in kinds.items() if kind in {"plan", "position"}}
    for name, selected in class_jobs.items():
        selected.sort(key=lambda job: job["job_ordinal"])
        if any(left["finished_ns"] > right["started_ns"] for left, right in zip(selected, selected[1:])):
            errors.append("one maintenance worker overlapped or reordered its sweep jobs")
        kind = "global_" + name
        if selected:
            transaction_kinds[kind] = sum(job["batches"] for job in selected)
            outcomes = run.get("per_kind", {}).get(kind, {}).get("outcomes", {})
            if any(outcomes.get(field) != sum(job["outcomes"][field] for job in selected) for field in OUTCOME_FIELDS):
                errors.append("maintenance sweep effects do not reconcile with per-kind outcomes")
        worker = config["workers"] + int(name == "housekeeping")
        activity = run.get("worker_activity", [])
        if (len(activity) <= worker or activity[worker].get("retries") != sum(job["retries"] for job in selected)
                or activity[worker].get("busy_ns") != sum(job["finished_ns"] - job["started_ns"] for job in selected)):
            errors.append("maintenance sweep worker retries or occupied duration differ from whole jobs")
    for name in ("projection", "housekeeping", "maintenance"):
        selected = jobs if name == "maintenance" else class_jobs[name]
        statistics = run.get("workload_classes", {}).get(name, {})
        if (statistics.get("retries") != sum(job["retries"] for job in selected)
                or statistics.get("positive_effect_jobs") != sum(job["processed_rows"] > 0 for job in selected)):
            errors.append("maintenance sweep class retries or useful job counts differ from completed jobs")
        for field, end, start in (("p99_us_including_retries", "received_ns", "scheduled_ns"),
                                  ("service_latency_p99_us_including_retries", "finished_ns", "started_ns"),
                                  ("arrival_queue_delay_p99_us", "started_ns", "scheduled_ns")):
            durations = sorted(job[end] - job[start] for job in selected)
            expected = durations[(len(durations) * 99 + 99) // 100 - 1] / 1000 if durations else None
            value = statistics.get(field)
            if (expected is None and value is not None) or (expected is not None and (not finite_number(value) or not math.isclose(value, expected, rel_tol=1e-9, abs_tol=1e-9))):
                errors.append("maintenance sweep latency differs from complete job timestamps")
    completed_transactions = corpus["foreground"] + totals["committed_batches"]
    if (type(run.get("completed_transactions")) is not int or run["completed_transactions"] != completed_transactions
            or not counts_match(run.get("transaction_kinds"), transaction_kinds)
            or not counts_match({kind: value.get("transactions") for kind, value in per_kind.items()}, transaction_kinds)):
        errors.append("maintenance sweep committed transaction counts do not reconcile with foreground and batch receipts")
    return errors


def calibrated_report_errors(run: dict, config: dict, expected_counts: list[int]) -> list[str]:
    """Check calibrated scheduling evidence independently of cadence realism."""
    errors = []
    foreground_workers = config["workers"]
    foreground = config["arrival_rate"] * config["seconds"]
    projection, housekeeping = calibrated_timer_counts(config)
    expected_classes = {"foreground": foreground, "projection": projection,
                        "housekeeping": housekeeping, "maintenance": projection + housekeeping}
    if run.get("total_process_workers") != foreground_workers + 2 or run.get("offered_rate_scope") != "foreground_only":
        errors.append("calibrated worker count or foreground offered-rate scope differs")
    schedule = run.get("calibrated_schedule", {})
    if not isinstance(schedule, dict):
        return errors + ["calibrated schedule is missing or malformed"]
    corpus = calibrated_corpus(config)
    dispatch, ttl, pattern = dispatch_config(config)
    maintenance_mode, projection_limit, housekeeping_limit, _ = maintenance_config(config)
    intervals = [config[name] for name in CALIBRATED_FIELDS]
    cadence = ("accelerated" if min(intervals) < 300 else
               "representative_interval_config" if max(intervals) <= 600 else "custom_outside_calibration")
    required = {"foreground_workers": foreground_workers, "maintenance_workers": 2,
                "active_families": corpus["active_families"], "quiet_families": corpus["quiet_families"],
                "projection_interval_seconds": config["projection_interval_seconds"],
                "housekeeping_interval_seconds": config["housekeeping_interval_seconds"],
                "first_timer_tick": "after_one_interval", "timer_admission": "strictly_before_end",
                "clock": "wall_clock", "cadence": cadence,
                "per_flight_ordering": "stable_foreground_worker_fifo" if dispatch == "identity" else "signature_affinity_worker_fifo",
                "projection_batch_limit": projection_limit, "housekeeping_batch_limit": housekeeping_limit,
                "maintenance_scope": "complete_sweep_batched_transactions" if maintenance_mode == "sweep" else "bounded_batch_not_full_sweep",
                "population_turnover_tested": False,
                "global_maintenance_sweep_complete": maintenance_mode == "sweep" and projection + housekeeping > 0}
    # Historical reports predate configurable batches; only the exact default
    # control may omit the mode and cap fields.
    for name, default in (("maintenance_mode", "batch"), ("max_maintenance_batches", 4096)):
        if type(schedule.get(name, default)) is not type(config_value(config, name)) or schedule.get(name, default) != config_value(config, name):
            errors.append("calibrated maintenance mode or batch cap differs from configuration")
    if any(type(schedule.get(name)) is not type(value) or schedule[name] != value for name, value in required.items()):
        errors.append("calibrated timer/order/batch schedule differs from the declared profile")
    ordering = run.get("per_flight_order", {})
    if (not isinstance(ordering, dict) or ordering.get("checked") is not True
            or ordering.get("foreground_completions") != foreground
            or ordering.get("identities_observed") != min(foreground, corpus["active_families"])):
        errors.append("same-flight foreground ordering check failed, missing or incomplete")
    elif dispatch == "identity":
        if ordering.get("passed") is not True or ordering.get("required", True) is not True:
            errors.append("identity dispatch did not preserve required same-flight FIFO")
    else:
        diagnostics = [ordering.get(name) for name in ("overlapping_messages", "out_of_order_completions")]
        if (ordering.get("required") is not False or type(ordering.get("passed")) is not bool
                or any(type(value) is not int or not 0 <= value <= foreground for value in diagnostics)
                or ordering["passed"] != all(value == 0 for value in diagnostics)):
            errors.append("affinity same-flight ordering diagnostics are missing or inconsistent")
    audit = run.get("dispatch_audit")
    # Historic identity/both reports predate routing fingerprints. Their closed
    # form worker counts and strict FIFO checks retain their original meaning.
    if audit is not None or dispatch != "identity" or pattern != "both":
        expected_audit = calibrated_dispatch(config)
        if (not isinstance(audit, dict) or audit.get("checked") is not True or audit.get("passed") is not True
                or any(type(audit.get(name)) is not type(value) or audit[name] != value
                       for name, value in expected_audit.items())):
            errors.append("dispatch assignments/counters differ from independent offered-input routing")
    classes = run.get("workload_classes", {})
    if not isinstance(classes, dict) or set(classes) != set(expected_classes):
        return errors + ["calibrated foreground/maintenance class metrics are missing"]
    for name, expected in expected_classes.items():
        statistics = classes[name]
        if not isinstance(statistics, dict):
            errors.append(f"invalid calibrated class metrics: {name}")
            continue
        for field in ("offered", "completed", "retries", "positive_effect_jobs"):
            if type(statistics.get(field)) is not int or statistics[field] < 0:
                errors.append(f"invalid calibrated {name} {field}")
        if statistics.get("offered") != expected or statistics.get("completed") != expected:
            errors.append(f"calibrated {name} omitted, duplicated or changed scheduled work")
        if not expected and statistics.get("retries") != 0:
            errors.append(f"calibrated empty {name} class invents retries")
        positive = statistics.get("positive_effect_jobs")
        if type(positive) is int and positive > expected:
            errors.append(f"calibrated {name} has more positive jobs than completions")
        for field in ("p99_us_including_retries", "service_latency_p99_us_including_retries", "arrival_queue_delay_p99_us"):
            value = statistics.get(field)
            if (expected and (not finite_number(value) or value < 0)) or (not expected and value is not None):
                errors.append(f"calibrated {name} latency is missing/invalid or invents zero-count samples")
    if errors:
        return errors
    if (sum(classes[name]["retries"] for name in ("foreground", "projection", "housekeeping")) != run.get("retries")
            or classes["maintenance"]["retries"] != classes["projection"]["retries"] + classes["housekeeping"]["retries"]
            or classes["maintenance"]["positive_effect_jobs"] != classes["projection"]["positive_effect_jobs"] + classes["housekeeping"]["positive_effect_jobs"]):
        errors.append("calibrated retry or maintenance effect counts do not reconcile")
    activity = run.get("worker_activity", [])
    if not isinstance(activity, list) or len(activity) != len(expected_counts):
        return errors + ["calibrated per-worker activity missing or incomplete"]
    total_elapsed = run.get("elapsed_seconds_including_drain")
    for worker, (statistics, expected) in enumerate(zip(activity, expected_counts)):
        role = "foreground" if worker < foreground_workers else ("projection" if worker == foreground_workers else "housekeeping")
        if (not isinstance(statistics, dict) or statistics.get("worker") != worker or statistics.get("role") != role
                or statistics.get("offered") != expected or statistics.get("completed") != expected
                or any(type(statistics.get(field)) is not int or statistics[field] < 0 for field in ("retries", "busy_ns"))):
            errors.append("calibrated worker roles/counts/activity differ from offered work")
            continue
        utilization = statistics.get("utilization")
        if not expected and (statistics["busy_ns"] != 0 or statistics["retries"] != 0):
            errors.append("idle calibrated worker invents activity or retries")
        if (not finite_number(total_elapsed) or total_elapsed <= 0
                or not finite_number(utilization) or not 0 <= utilization <= 1
                or not math.isclose(utilization, statistics["busy_ns"] / (total_elapsed * 1e9), rel_tol=1e-9, abs_tol=1e-9)):
            errors.append("calibrated worker utilization differs from service time / continuous duration")
    if not errors and sum(worker["retries"] for worker in activity) != run.get("retries"):
        errors.append("calibrated worker retry counts differ from the total")
    if not errors:
        errors.extend(maintenance_report_errors(run, config))
    return errors


def assess_calibrated_scope(result: dict, run: dict, config: dict, policy: dict) -> dict:
    """Keep steady-state diagnostic budgets separate from capacity claims.

    This profile covers timer cadence, foreground ordering and optional batched
    sweeps, but deliberately omits population turnover. Even a long run
    at representative intervals therefore cannot supply a capacity bound yet.
    """
    classes = run["workload_classes"]
    foreground = classes["foreground"]["completed"]
    foreground_outcomes = {}
    for kind in ("plan", "position"):
        for name, value in run.get("per_kind", {}).get(kind, {}).get("outcomes", {}).items():
            foreground_outcomes[name] = foreground_outcomes.get(name, 0) + value
    missing = foreground_outcomes.get("missing_family", 0) + foreground_outcomes.get("allocation_deferred", 0)
    population = [run.get(snapshot, {}).get("live_families") for snapshot in ("initial_fleet", "final_fleet")]
    populated = all(type(count) is int and count == config["families"] for count in population)
    foreground_complete = (missing == 0 and foreground_outcomes.get("ignored_stale", 0) == 0
                           and foreground_outcomes.get("duplicate_messages", 0) == 0
                           and classes["foreground"]["positive_effect_jobs"] == foreground)
    useful = foreground_complete and foreground_outcomes.get("updated_views", 0) > 0 and populated
    representative = all(300 <= config[name] <= 600 for name in CALIBRATED_FIELDS)
    repeated = all(classes[name]["completed"] >= 2 for name in ("projection", "housekeeping"))
    positive = all(classes[name]["positive_effect_jobs"] >= 2 for name in ("projection", "housekeeping"))
    cadence_covered = representative and repeated and positive and result["continuous_timing_passed"]
    elapsed = run.get("elapsed_seconds_including_drain")
    foreground_rate = foreground / elapsed if finite_number(elapsed) and elapsed > 0 else None
    slo = classes["foreground"]["p99_us_including_retries"] <= policy["slo_ms"] * 1000
    rate = foreground_rate is not None and foreground_rate >= config["arrival_rate"] * policy["minimum_drain_fraction"]
    view_updates = foreground_outcomes.get("updated_views", 0) + foreground_outcomes.get("ignored_stale", 0)
    dispatch, _, _ = dispatch_config(config)
    sweep = config_value(config, "maintenance_mode") == "sweep"
    result.update(
        no_op_fraction=missing / foreground, useful_work_passed=useful,
        foreground_outcomes=foreground_outcomes,
        foreground_effect_coverage_passed=foreground_complete,
        foreground_positive_job_fraction=classes["foreground"]["positive_effect_jobs"] / foreground,
        foreground_stale_view_update_fraction=foreground_outcomes.get("ignored_stale", 0) / view_updates if view_updates else None,
        foreground_p99_ms=classes["foreground"]["p99_us_including_retries"] / 1000,
        foreground_throughput_with_drain=foreground_rate,
        fleet_population_snapshots_passed=populated,
        representative_cadence_config=representative,
        repeated_maintenance_occurrences=repeated, repeated_positive_maintenance_jobs=positive,
        representative_cadence_coverage_passed=cadence_covered,
        maintenance_jobs={name: {field: classes[name][field] for field in ("offered", "completed", "positive_effect_jobs")}
                          for name in ("projection", "housekeeping")},
        foreground_ordering_required=dispatch == "identity",
        foreground_ordering_passed=run["per_flight_order"]["passed"],
        diagnostic_performance_passed=result["continuous_timing_passed"] and useful and slo and rate,
        population_turnover_tested=False,
        global_maintenance_sweep_complete=sweep and classes["maintenance"]["completed"] > 0,
        calibrated_capacity_qualification_complete=False,
        performance_passed=False, qualified_capacity_trial=False, capacity_failure=False,
        qualification_limitations=["steady-state population omits lifecycle turnover",
                                   "other background cadences and production workload distributions remain uncalibrated"]
                                 + ([] if sweep else ["maintenance jobs are bounded batches, not complete global sweeps"]),
    )
    if not useful:
        result["reasons"].append("calibrated foreground has missing/deferred/stale/duplicate or effectless inputs, lacks updates, or changes its seeded population")
    if not representative:
        result["reasons"].append("accelerated/out-of-range maintenance intervals do not represent five-to-ten-minute cadence")
    if not repeated or not positive:
        result["reasons"].append("cadence coverage needs at least two completed positive-effect jobs of each timer kind")
    if not slo:
        result["reasons"].append("declared foreground end-to-end p99 SLO exceeded")
    if not rate:
        result["reasons"].append("drained foreground completion rate below declared offered-rate fraction")
    if not result["correctness_companion_verified"]:
        result["reasons"].append("no exact source/binary/configuration/corpus full-history companion")
    return result


def compatible_companion(companion: dict | None, source: dict, binary_sha: str) -> bool:
    return bool(companion and companion.get("format") == FORMAT
                and companion.get("completed") is True and companion.get("source_stable") is True
                and companion.get("source_before") == source == companion.get("source_after")
                and companion.get("binary_before_sha256") == binary_sha == companion.get("binary_after_sha256"))


def collect_outcomes(run: dict) -> dict:
    totals = {}
    for kind, statistics in run.get("per_kind", {}).items():
        if not isinstance(statistics, dict) or not isinstance(statistics.get("outcomes"), dict):
            raise ValueError(f"missing outcome counts for {kind}")
        for name, count in statistics["outcomes"].items():
            if type(count) is not int or count < 0:
                raise ValueError("invalid outcome counter")
            totals[name] = totals.get(name, 0) + count
    return totals


def lifecycle_kind_counts(offered: int) -> dict:
    kinds = ("plan", "position", "plan", "position", "plan", "position", "global_projection", "global_reschedule", "arrival", "arrival", "arrival", "expire_family", "global_housekeeping", "expire_family", "global_cancel", "global_housekeeping")
    counts = {}
    for step, kind in enumerate(kinds):
        count = offered // 16 + int(step < offered % 16)
        if count:
            counts[kind] = counts.get(kind, 0) + count
    return counts


def expected_kind_counts(config: dict, offered: int) -> dict:
    """Independently enumerate the declared fleet schedule, without Rust output.

    Warmup is sixteen complete identity rounds. Each timed round visits every
    identity in a triangularly rotated order; identity age selects its phase.
    This count depends only on the offered corpus, never on worker completion.
    """
    if config.get("workload") == "lifecycle":
        return lifecycle_kind_counts(offered)
    if config.get("workload") == "calibrated":
        corpus = calibrated_corpus(config)
        if offered != corpus["total"]:
            raise ValueError("calibrated total differs from foreground plus independently scheduled jobs")
        return corpus["kinds"]
    if config.get("workload") != "fleet":
        raise ValueError("unsupported capacity workload")
    families = config["families"]
    if families < 16 or config["hot_percent"] != 0:
        raise ValueError("fleet requires at least 16 identities and hot-percent=0")
    phases = ("plan", "position", "plan", "position", "plan", "position", "global_projection", "global_reschedule", "arrival", "arrival", "arrival", "expire_family", "global_housekeeping", "expire_family", "global_cancel", "global_housekeeping")
    counts = {}
    remaining = offered
    current_round = 16
    while remaining:
        rotation = current_round * (current_round + 1) // 2 % families
        for slot in range(min(remaining, families)):
            identity = (slot + rotation) % families
            phase = (current_round + identity % 16 + config["seed"] % 16) % 16
            kind = phases[phase]
            counts[kind] = counts.get(kind, 0) + 1
        remaining -= min(remaining, families)
        current_round += 1
    return counts


def continuous_timing_errors(run: dict, completed: int) -> list[str]:
    """Validate one continuous client-clock interval through confirmed drain.

    This is capacity evidence, independent of a successful serial witness.
    Older reports can retain valid histories without qualifying throughput.
    """
    errors = []
    if run.get("timing_scope") != "continuous_client_monotonic":
        errors.append("missing continuous client-monotonic drain timing scope")
    names = ("admission_started_ns", "workload_completed_ns", "workers_stopped_ns", "drain_confirmed_ns")
    timestamps = [run.get(name) for name in names]
    if not all(type(value) is int and 0 <= value < 2**64 for value in timestamps):
        errors.append("missing or invalid continuous drain timestamps")
        return errors
    admission, workload, stopped, drained = timestamps
    if not admission < workload <= stopped <= drained:
        errors.append("continuous drain timestamps are not ordered")
        return errors
    measured_workload = (workload - admission) / 1e9
    measured_total = (drained - admission) / 1e9
    for name, measured in (("elapsed_seconds", measured_workload),
                           ("elapsed_seconds_including_drain", measured_total)):
        actual = run.get(name)
        if (not finite_number(actual) or actual <= 0
                or not math.isclose(actual, measured, rel_tol=1e-9, abs_tol=1e-9)):
            errors.append(f"{name} differs from its continuous timestamp interval")
    throughput = run.get("completed_messages_per_second_including_drain")
    if (not finite_number(throughput) or throughput <= 0
            or not math.isclose(throughput, completed / measured_total, rel_tol=1e-9, abs_tol=1e-9)):
        errors.append("drained throughput differs from completed messages / continuous elapsed time")
    return errors


def retry_trace_errors(trace: object, enabled: bool) -> list[str]:
    """Validate bounded evidence without calling an incomplete cache complete."""
    def uint(value):
        return type(value) is int and 0 <= value < 2**64
    if not isinstance(trace, dict):
        return ["missing worker retry diagnostics"]
    if (trace.get("version") != 1 or type(trace.get("version")) is not int
            or trace.get("enabled") is not enabled
            or type(trace.get("sample_limit")) is not int or trace.get("sample_limit") != 32
            or not uint(trace.get("failed_attempts")) or not uint(trace.get("dropped_attempts"))
            or not isinstance(trace.get("samples"), list)):
        return ["malformed retry diagnostic header"]
    samples = trace["samples"]
    if (len(samples) != (min(32, trace["failed_attempts"]) if enabled else 0)
            or trace["dropped_attempts"] != trace["failed_attempts"] - len(samples)):
        return ["retry diagnostic truncation or disabled scope is inconsistent"]
    errors = []
    previous = None
    for item in samples:
        if not isinstance(item, dict) or not all(uint(item.get(field)) for field in
                ("message_id", "attempt_index", "started_ns", "finished_ns", "cleanup_finished_ns")):
            errors.append("malformed failed-attempt identity/clock")
            continue
        if (item["attempt_index"] > 128
                or not item["started_ns"] <= item["finished_ns"] <= item["cleanup_finished_ns"]):
            errors.append("invalid failed-attempt ordinal or timing")
        if previous is not None:
            if item["started_ns"] < previous["cleanup_finished_ns"]:
                errors.append("failed-attempt intervals overlap")
            if item["message_id"] == previous["message_id"] and item["attempt_index"] != previous["attempt_index"] + 1:
                errors.append("failed-attempt retry ordinals skip within a retained message")
            if item["message_id"] != previous["message_id"] and item["attempt_index"] != 0:
                errors.append("new retained message does not begin with its original attempt")
        previous = item
        if (item.get("error_kind") not in {"conflict", "fatal"}
                or not isinstance(item.get("error"), str) or len(item["error"]) > 512
                or type(item.get("cleanup_ok")) is not bool
                or (item.get("cleanup_error") is not None and
                    (not isinstance(item["cleanup_error"], str) or len(item["cleanup_error"]) > 512))
                or item.get("cleanup_ok") != (item.get("cleanup_error") is None)
                or type(item.get("counter_regression")) is not bool):
            errors.append("malformed failed-attempt error/cleanup evidence")
        if item.get("counter_regression") is True:
            errors.append("retry diagnostic cumulative counters regressed")
        for field in ("retry_causes_delta", "diagnostics_delta", "cleanup_retry_causes_delta", "cleanup_diagnostics_delta"):
            counts = item.get(field)
            if not isinstance(counts, dict) or any(not isinstance(key, str) or not uint(value) for key, value in counts.items()):
                errors.append("malformed failed-attempt counter delta")
        status = item.get("metrics_status")
        if not isinstance(status, dict) or type(status.get("complete")) is not bool:
            errors.append("missing failed-attempt counter freshness")
        elif status.get("source") == "local_adapter":
            if status["complete"] is not True:
                errors.append("local adapter snapshot incorrectly scoped")
        elif status.get("source") == "service_cache":
            fields = ("last_attempted_sequence", "last_completed_sequence", "last_metrics_sequence")
            if (any(status.get(field) is not None and not uint(status[field]) for field in fields)
                    or type(status.get("transport_failed")) is not bool
                    or type(status.get("connected")) is not bool):
                errors.append("malformed service counter freshness")
            else:
                complete = (status.get("last_metrics_sequence") is not None
                            and status["last_metrics_sequence"] == status["last_completed_sequence"] == status["last_attempted_sequence"]
                            and not status["transport_failed"])
                if status["complete"] != complete:
                    errors.append("stale service counters reported complete")
        else:
            errors.append("unknown failed-attempt counter source")
    return errors


def experiment_report_errors(run: dict, config: dict) -> list[str]:
    policy, enabled = (config_value(config, field) for field in EXPERIMENT_DEFAULTS)
    if policy not in {"all-active", "housekeeping"} or type(enabled) is not bool:
        return ["invalid expiry/diagnostic experiment configuration"]
    errors = []
    modern = "worker_retry_diagnostics" in run
    if run.get("expiry_index_policy", "all-active") != policy:
        errors.append("reported native expiry eligibility differs from configuration")
    expected_policy = "housekeeping" if config.get("engine") == "postgres" else policy
    if (modern or "effective_expiry_index_policy" in run) and run.get("effective_expiry_index_policy") != expected_policy:
        errors.append("reported effective expiry eligibility differs from engine configuration")
    if enabled and run.get("retry_diagnostics_compiled") is not True:
        errors.append("diagnostic mode lacks a diagnostic-feature binary")
    traces = run.get("worker_retry_diagnostics")
    if not modern and not enabled and policy == "all-active":
        return errors  # Legacy default reports do not contain the optional trace.
    workers = run.get("completed_by_worker", [])
    if not isinstance(traces, list) or not isinstance(workers, list) or len(traces) != len(workers):
        return errors + ["missing per-worker retry diagnostic coverage"]
    for trace in traces:
        trace_errors = retry_trace_errors(trace, enabled)
        errors.extend(trace_errors)
        if not trace_errors and any(item["error_kind"] != "conflict" or not item["cleanup_ok"]
                or not item["metrics_status"]["complete"] or item["attempt_index"] >= 128
                for item in trace["samples"]):
            errors.append("completed worker contains a terminal failed attempt")
    if not errors and sum(trace["failed_attempts"] for trace in traces) != run.get("retries"):
        errors.append("successful-run failed attempts differ from completed transaction retries")
    return errors


def assess_trial(trial: dict, policy: dict, companion_keys: set[tuple] = frozenset()) -> dict:
    """Fail closed on incomplete input coverage, timing, errors or bad histories.

    A performance failure is separate from invalid evidence: only reproducible
    latency/drain-rate failures can establish a tested saturation upper bound.
    """
    result = {"execution_valid": False, "performance_passed": False,
              "correctness_companion_verified": False, "history_verified": False,
              "qualified_capacity_trial": False, "capacity_failure": False,
              "continuous_timing_passed": False, "reasons": []}
    reasons = result["reasons"]
    config = trial.get("config", {})
    report = trial.get("report", {})
    if trial.get("exit_code") != 0 or trial.get("timed_out") or trial.get("error"):
        reasons.append("process failed, timed out, or lacked a readable report")
    if not trial.get("source_stable", False):
        reasons.append("source or binary changed during trial")
    if report.get("completed") is not True or report.get("passed") is not True:
        reasons.append("benchmark did not complete successfully")
    actual = report.get("config", {})
    if actual.get("mode") != "sustained" or any(type(config_value(actual, field)) is not type(config_value(config, field)) or config_value(actual, field) != config_value(config, field) for field in match_fields(config) + ("evidence",)):
        reasons.append("reported configuration differs from offered corpus/configuration")
    if config.get("workload") not in {"lifecycle", "fleet", "calibrated"} or not finite_number(config.get("arrival_rate")) or config.get("arrival_rate", 0) <= 0:
        reasons.append("assessment requires a supported fixed corpus with positive offered arrivals")
    runs = report.get("runs", [])
    if not isinstance(runs, list) or len(runs) != 1 or not isinstance(runs[0], dict):
        reasons.append("expected exactly one sustained run")
        return result
    run = runs[0]
    reasons.extend(experiment_report_errors(run, config))
    if run.get("engine") != config.get("engine") or run.get("scenario") != "sustained-mixed":
        reasons.append("wrong engine or sustained scenario")
    if run.get("passed") is not True or run.get("execution_completed") is not True or run.get("error"):
        reasons.append("run failed correctness, progress, or execution")
    expected = config.get("arrival_rate", 0) * config.get("seconds", 0)
    workers = config.get("workers", 0)
    calibrated = config.get("workload") == "calibrated"
    counts = run.get("completed_by_worker", [])
    expected_counts = [(expected - 1 - worker) // workers + 1 if expected > worker else 0 for worker in range(workers)] if type(workers) is int and workers > 0 else []
    if calibrated:
        try:
            corpus = calibrated_corpus(config)
            expected, expected_counts = corpus["total"], corpus["worker_counts"]
            reasons.extend(calibrated_report_errors(run, config, expected_counts))
        except (ValueError, TypeError, KeyError) as error:
            reasons.append(str(error))
            return result
    process_workers = workers + 2 if calibrated else workers
    arrival_mode = "calibrated_fixed_timeline" if calibrated else "independent_fixed_corpus"
    if (run.get("arrival_mode") != arrival_mode or run.get("offered_messages") != expected
            or run.get("completed_messages") != expected or counts != expected_counts
            or not expected_counts or (not calibrated and min(expected_counts) <= 0)):
        reasons.append("offered corpus was omitted, duplicated, truncated, or changed")
    if (run.get("admission_seconds") != config.get("seconds")
            or run.get("offered_rate_per_second") != config.get("arrival_rate")
            or run.get("requested_duration_reached") is not True or run.get("worker_message_cap_reached") is not False
            or not finite_number(run.get("elapsed_seconds")) or run.get("elapsed_seconds", 0) < config.get("seconds", 0)):
        reasons.append("full admission duration was not observed")
    stops = run.get("worker_stops", [])
    if len(stops) != process_workers or any(s.get("stop_reason") != "arrival_corpus_drained" for s in stops):
        reasons.append("a worker did not drain the fixed corpus")
    if run.get("invariants", {}).get("passed") is not True:
        reasons.append("final structural invariants failed or missing")
    expected_commits = run.get("completed_transactions") if calibrated and config_value(config, "maintenance_mode") == "sweep" else expected
    if type(expected_commits) is not int or type(run.get("store_metrics", {}).get("commits")) is not int or run.get("store_metrics", {}).get("commits") != expected_commits:
        reasons.append("commit count differs from complete committed transaction receipts")
    full = (config.get("evidence") == "full" and run.get("history_checked") is True
            and run.get("correctness_history_verified") is True and run.get("oracle_status") == "Valid")
    if config.get("evidence") == "full" and not full:
        reasons.append("full-history oracle failed, was inconclusive, or was not run")
    if config.get("evidence") == "metrics" and (run.get("history_checked") is not False or run.get("correctness_history_verified") is not False or run.get("oracle_status") != "NotCheckedMetricsOnly"):
        reasons.append("metrics-only report misstates history verification")
    try:
        outcomes = collect_outcomes(run)
        if not all(name in outcomes for name in EFFECT_FIELDS + ("missing_family", "allocation_deferred", "ignored_stale")):
            raise ValueError("missing required lifecycle effect counters")
        per_kind_completed = sum(value.get("completed", -expected) for value in run.get("per_kind", {}).values())
        if per_kind_completed != expected:
            raise ValueError("per-kind completion counts differ from corpus")
        expected_kinds = expected_kind_counts(config, expected)
        if (run.get("message_kinds") != expected_kinds
                or {name: value.get("completed") for name, value in run.get("per_kind", {}).items()} != expected_kinds):
            raise ValueError("message kind counts differ from the declared corpus")
    except (ValueError, TypeError) as error:
        reasons.append(str(error))
        outcomes = {}
    latency = run.get("message_latency_p99_us_including_retries")
    throughput = run.get("completed_messages_per_second_including_drain")
    if not finite_number(latency) or latency < 0 or not finite_number(throughput) or throughput <= 0:
        reasons.append("missing or invalid end-to-end latency/drained throughput")
    result.update(outcomes=outcomes, completed_messages=run.get("completed_messages"),
                  completed_transactions=run.get("completed_transactions", run.get("completed_messages")),
                  p99_ms=latency / 1000 if finite_number(latency) else None,
                  throughput_with_drain=throughput, per_kind=run.get("per_kind", {}),
                  configured_identities=config.get("families"),
                  initial_observed_live_families=run.get("initial_fleet", {}).get("live_families"),
                  final_observed_live_families=run.get("final_fleet", {}).get("live_families"),
                  population_scope="initial/final snapshots; not a runtime minimum")
    result["execution_valid"] = not reasons
    result["history_verified"] = full and not reasons
    result["correctness_companion_verified"] = not reasons and (full or key(config) in companion_keys)
    if reasons:
        return result
    timing_errors = continuous_timing_errors(run, expected)
    timing_passed = not timing_errors
    result.update(continuous_timing_passed=timing_passed, continuous_timing_errors=timing_errors,
                  elapsed_seconds_including_drain=run.get("elapsed_seconds_including_drain"),
                  timing_scope=run.get("timing_scope"))
    reasons.extend(timing_errors)
    if calibrated:
        return assess_calibrated_scope(result, run, config, policy)
    noops = outcomes.get("missing_family", 0) + outcomes.get("allocation_deferred", 0)
    result["no_op_fraction"] = noops / expected
    useful = (result["no_op_fraction"] <= policy["max_noop_fraction"] and outcomes.get("created_views", 0) > 0 and outcomes.get("expired_families", 0) > 0)
    if config.get("workload") == "fleet":
        population = [result["initial_observed_live_families"], result["final_observed_live_families"]]
        populated = all(type(count) is int and config["families"] // 2 <= count <= 2 * config["families"] for count in population)
        full_cycle = expected >= 16 * config["families"]
        kinds = run.get("per_kind", {})
        projection = kinds.get("global_projection", {}).get("outcomes", {}).get("claimed_events", 0) > 0
        rescheduling = kinds.get("global_reschedule", {}).get("outcomes", {}).get("rescheduled_events", 0) > 0
        cancellation = kinds.get("global_cancel", {}).get("outcomes", {}).get("cancelled_events", 0) > 0
        housekeeping = kinds.get("global_housekeeping", {}).get("outcomes", {}).get("expired_records", 0) > 0
        background = projection and rescheduling and cancellation and housekeeping
        useful = useful and populated and full_cycle and background
        result.update(fleet_population_snapshots_passed=populated, fleet_complete_phase_cycle=full_cycle,
                      fleet_global_projection_mutated=projection, fleet_global_rescheduling_mutated=rescheduling,
                      fleet_global_cancellation_mutated=cancellation, fleet_global_housekeeping_mutated=housekeeping)
        if not populated:
            reasons.append("fleet initial/final population snapshot missing or below configured-identity threshold")
        if not full_cycle:
            reasons.append("fleet capacity needs at least sixteen rounds of configured identities")
        if not background:
            reasons.append("fleet projection/rescheduling/cancellation/housekeeping did not all mutate records")
    result["useful_work_passed"] = useful
    if not useful:
        reasons.append("lifecycle lacks creation/expiry turnover or exceeds missing/deferred limit")
    slo = latency <= policy["slo_ms"] * 1000
    rate = throughput >= config["arrival_rate"] * policy["minimum_drain_fraction"]
    if not slo:
        reasons.append("declared end-to-end p99 SLO exceeded")
    if not rate:
        reasons.append("drained completion rate below declared offered-rate fraction")
    result["performance_passed"] = timing_passed and useful and slo and rate
    result["qualified_capacity_trial"] = result["performance_passed"] and result["correctness_companion_verified"]
    result["capacity_failure"] = timing_passed and useful and result["correctness_companion_verified"] and not (slo and rate)
    if not result["correctness_companion_verified"]:
        reasons.append("no exact source/binary/configuration/corpus full-history companion")
    return result


def build_gate(trials: list[dict], policy: dict, engines: list[str], rates: list[int], workers: list[int], seeds: list[int], companion_keys: set[tuple] = frozenset()) -> dict:
    """Require every declared seed; optimize only across declared worker counts."""
    assessed = []
    axes = {"engine", "workers", "arrival_rate", "seed"}
    common = {tuple((field, config_value(trial["config"], field)) for field in match_fields(trial["config"]) if field not in axes)
              for trial in trials}
    corpus_configuration_matches = len(common) == 1
    for trial in trials:
        assessment = assess_trial(trial, policy, companion_keys)
        if not corpus_configuration_matches:
            assessment.update(execution_valid=False, performance_passed=False, qualified_capacity_trial=False, capacity_failure=False)
            assessment["reasons"].append("matrix changes corpus/configuration beyond the declared rate/worker/seed/engine axes")
        assessed.append({"config": trial["config"], "assessment": assessment})
    grid = {}
    for item in assessed:
        cfg = item["config"]
        cell = (cfg["engine"], cfg["arrival_rate"], cfg["workers"], cfg["seed"])
        grid.setdefault(cell, []).append(item["assessment"])
    def all_seeds(engine, rate, worker, predicate):
        return all(len(grid.get((engine, rate, worker, seed), [])) == 1 and grid[(engine, rate, worker, seed)][0].get(predicate) is True for seed in seeds)
    bounds = {}
    for engine in engines:
        passing = [(rate, worker) for rate in rates for worker in workers if len(seeds) >= 3 and all_seeds(engine, rate, worker, "qualified_capacity_trial")]
        exploratory = [(rate, worker) for rate in rates for worker in workers if all_seeds(engine, rate, worker, "performance_passed")]
        best = max(passing, key=lambda pair: (pair[0], -pair[1]), default=None)
        upper = [rate for rate in rates if best and rate > best[0] and all(all_seeds(engine, rate, worker, "capacity_failure") for worker in workers)]
        bounds[engine] = {"best_tested_qualified_rate": best[0] if best else None,
                          "selected_workers": best[1] if best else None,
                          "best_tested_exploratory_rate": max((r for r, _ in exploratory), default=None),
                          "failing_tested_upper_rate": min(upper, default=None),
                          "all_seed_passes_required": True, "three_or_more_seeds": len(seeds) >= 3,
                          "worker_search_exhaustive_only_within_declared_grid": True}
    comparisons = {}
    pg = bounds.get("postgres", {})
    for engine, bound in bounds.items():
        if engine == "postgres":
            continue
        lower, pg_lower, upper = bound["best_tested_qualified_rate"], pg.get("best_tested_qualified_rate"), pg.get("failing_tested_upper_rate")
        exploratory_a = bound["best_tested_exploratory_rate"]
        exploratory_pg = pg.get("best_tested_exploratory_rate")
        comparison = {"ratio_of_tested_passing_rates": exploratory_a / exploratory_pg if exploratory_a and exploratory_pg else None,
                      "ratio_is_capacity_proof": False, "synthetic_capacity_lower_bound": None,
                      "synthetic_10x_bound_demonstrated": False,
                      "conditional_tested_grid_10x_ratio": False,
                      "hardware_resource_equivalence_verified": False,
                      "saturation_monotonicity_verified": False,
                      "useful_work_comparable": False,
                      "reason": "passing rates are lower bounds; need a repeated PostgreSQL saturation bracket and comparable useful work"}
        if lower and pg_lower and upper:
            # Compare useful work on an identical full corpus at the PG upper
            # rate; comparing different message counts at different rates alone
            # would conflate missing/deferred work with architecture capacity.
            candidates = [w for w in workers if all_seeds(engine, upper, w, "qualified_capacity_trial")]
            comparable = bool(candidates)
            for seed in seeds:
                if not comparable:
                    break
                a = grid[(engine, upper, candidates[0], seed)][0]
                for worker in workers:
                    b = grid[("postgres", upper, worker, seed)][0]
                    endpoint = grid[(engine, lower, bound["selected_workers"], seed)][0]
                    for name in EFFECT_FIELDS + ("missing_family", "allocation_deferred", "ignored_stale"):
                        av, bv = a["outcomes"].get(name, 0), b["outcomes"].get(name, 0)
                        if abs(av - bv) > policy["outcome_tolerance"] * max(av, bv, 1):
                            comparable = False
                        # Higher offered rates cover longer sequence prefixes;
                        # require comparable effects per completed input there
                        # as well, not only at the same-rate control point.
                        normalized_a = endpoint["outcomes"].get(name, 0) / endpoint["completed_messages"]
                        normalized_b = bv / b["completed_messages"]
                        if abs(normalized_a - normalized_b) > policy["outcome_tolerance"] * max(normalized_a, normalized_b, 1 / b["completed_messages"]):
                            comparable = False
            comparison["useful_work_comparable"] = comparable
            if comparable:
                comparison.update(synthetic_capacity_lower_bound=lower / upper,
                                  conditional_tested_grid_10x_ratio=lower >= 10 * upper and policy["outcome_tolerance"] == 0,
                                  reason="conditional synthetic ratio over the declared grid, seeds, duration, SLO and resource budget; strict grid flag requires zero outcome tolerance; a demonstrated capacity bound remains false because resource equivalence and saturation monotonicity are unproven")
        comparisons[engine] = comparison
    return {"trial_assessments": assessed, "capacity_bounds": bounds, "comparisons": comparisons,
            "corpus_configuration_matches": corpus_configuration_matches,
            "whole_engine_verified": False, "actual_hyperfeed_replacement_qualified": False,
            "architecture_promotion_eligible": False, "real_world_10x_claim": False,
            "scope": "exploratory synthetic capacity evidence; compatibility, recovery, MMHF, production retention and resource isolation remain unqualified"}


def descendants(parent: int) -> set[int]:
    parents = {}
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit():
            try:
                parents[int(entry.name)] = int((entry / "stat").read_text().rsplit(") ", 1)[1].split()[1])
            except (OSError, ValueError, IndexError):
                pass
    found = {parent}
    while True:
        extended = found | {pid for pid, ppid in parents.items() if ppid in found}
        if extended == found:
            return found - {parent}
        found = extended


def cleanup_private_configs(directory: Path, ownership_token: str) -> list[str]:
    """Remove credential configs only inside a token-marked owned trial tree.

    Directory symlinks are never followed. No other filename is removed, and
    the token guard prevents accidentally passing the whole output/repo root.
    Call only after the owned processes have terminated.
    """
    if not ownership_token or (directory / ".qualification-owned-trial").read_text() != ownership_token:
        raise ValueError("private-config cleanup requires the exact owned trial directory")
    removed = []
    for current, directories, filenames in os.walk(directory, followlinks=False):
        directories[:] = [name for name in directories if not (Path(current) / name).is_symlink()]
        for name in filenames:
            if re.fullmatch(r"(?:private-config|worker-[0-9]+)\.json", name):
                path = Path(current) / name
                path.unlink()
                removed.append(str(path.relative_to(directory)))
    return sorted(removed)


def live_owned_processes(group: int, known: set[int]) -> set[int]:
    live = set()
    for entry in Path("/proc").iterdir():
        if entry.name.isdigit():
            try:
                fields = (entry / "stat").read_text().rsplit(") ", 1)[1].split()
                pid = int(entry.name)
                if fields[0] not in {"Z", "X"} and (pid in known or int(fields[2]) == group):
                    live.add(pid)
            except (OSError, ValueError, IndexError):
                pass
    return live


def run_process(command: list[str], log: Path, timeout: float, secrets: tuple[str, ...] = (),
                owned_directory: Path | None = None, ownership_token: str | None = None) -> dict:
    """Bounded process-tree cleanup; private temporary stdout is scrubbed."""
    started = time.monotonic()
    timed_out = False
    error = None
    code = None
    terminated = False
    removed = []
    with tempfile.TemporaryFile(mode="w+b", dir=log.parent) as raw:
        process = subprocess.Popen(command, cwd=ROOT, stdout=raw, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
        except BaseException:
            error = "interrupted while waiting for benchmark"
            raise
        finally:
            children = set()
            if process.poll() is None:
                children = descendants(process.pid)
                for pid in children:
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                code = process.wait(timeout=5)
            # The Rust runner owns its nested coordinator groups. For forced
            # termination we captured and killed those descendants above; also
            # ensure no live member remains in the outer group after any exit.
            deadline = time.monotonic() + 5
            while True:
                live = live_owned_processes(process.pid, children)
                if not live:
                    terminated = True
                    break
                for pid in live:
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                if time.monotonic() >= deadline:
                    error = "owned process termination unconfirmed; private cleanup not attempted"
                    break
                time.sleep(.01)
            if terminated and owned_directory is not None:
                removed = cleanup_private_configs(owned_directory, ownership_token or "")
            raw.seek(0)
            content = raw.read().decode("utf-8", errors="replace")
            for secret in secrets:
                if secret:
                    content = content.replace(secret, "<redacted>")
            log.write_text(content)
    return {"exit_code": code, "timed_out": timed_out, "error": error, "wall_seconds": time.monotonic() - started,
            "owned_processes_terminated": terminated, "private_configs_removed": removed}


def host_info() -> dict:
    def read(path):
        try:
            return Path(path).read_text().strip()
        except OSError:
            return None
    return {"platform": platform.platform(), "machine": platform.machine(), "logical_cpus": os.cpu_count(),
            "cpu_affinity": sorted(os.sched_getaffinity(0)), "load_average": list(os.getloadavg()),
            "meminfo": read("/proc/meminfo"), "cgroup_cpu_max": read("/sys/fs/cgroup/cpu.max"),
            "cgroup_memory_max": read("/sys/fs/cgroup/memory.max"),
            "cpu_models": sorted({line.split(":", 1)[1].strip() for line in (read("/proc/cpuinfo") or "").splitlines() if line.startswith("model name")})}


def comma_ints(value: str) -> list[int]:
    try:
        values = [int(part) for part in value.split(",")]
        if not values or len(values) != len(set(values)) or min(values) < 0:
            raise ValueError()
        return values
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected distinct nonnegative comma-separated integers") from error


def main(argv=None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    # Invalidate even a configuration error; argparse otherwise exits before
    # the normal setup path. Help is informational and leaves prior data alone.
    output_hint = None
    for index, argument in enumerate(arguments):
        if argument == "--output" and index + 1 < len(arguments):
            output_hint = arguments[index + 1]
        elif argument.startswith("--output="):
            output_hint = argument.split("=", 1)[1]
    if output_hint and not any(arg in {"--help", "-h"} for arg in arguments):
        atomic_json(Path(output_hint).resolve() / "campaign.json",
                    {"format": FORMAT, "completed": False, "passed": False, "source_stable": False,
                     "stage": "configuration", "trials": [], "architecture_promotion_eligible": False})
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--pg-url-env", default="AEROSTORE_CONTENTION_PG_URL")
    parser.add_argument("--engines", default="aerostore,postgres")
    parser.add_argument("--workload", choices=["lifecycle", "fleet", "calibrated"], default="lifecycle")
    parser.add_argument("--projection-interval-seconds", type=int, default=300,
                        help="calibrated only: first tick after this interval; accelerated values are diagnostic only")
    parser.add_argument("--housekeeping-interval-seconds", type=int, default=600,
                        help="calibrated only: independent timer; 300..600 seconds matches operator cadence")
    parser.add_argument("--dispatch", choices=["identity", "signature-affinity"], default="identity")
    parser.add_argument("--affinity-ttl-ms", type=int, default=0,
                        help="required positive sliding TTL for signature-affinity; identity requires 0")
    parser.add_argument("--signature-pattern", choices=["both", "mixed"], default="both",
                        help="calibrated only: mixed repeats two both, two callsign-only and two tail-only inputs per flight")
    parser.add_argument("--maintenance-mode", choices=["batch", "sweep"], default="batch",
                        help="calibrated only: one bounded batch or successive transactions through an empty query")
    parser.add_argument("--projection-batch-size", type=int, default=4)
    parser.add_argument("--housekeeping-batch-size", type=int, default=32)
    parser.add_argument("--max-maintenance-batches", type=int, default=4096,
                        help="sweep transaction cap per job, including its required empty terminal transaction")
    parser.add_argument("--expiry-index", dest="expiry_index_policy", choices=["all-active", "housekeeping"], default="all-active", help="native fixture eligibility; PostgreSQL already uses housekeeping-only eligibility")
    parser.add_argument("--retry-diagnostics", choices=["off", "on"], default="off", help="bounded origin/failed-attempt evidence; on requires a diagnostic-feature binary")
    parser.add_argument("--rates", type=comma_ints, default=[32, 64])
    parser.add_argument("--workers", type=comma_ints, default=[1, 2])
    parser.add_argument("--seeds", type=comma_ints, default=[20260924, 20260925, 20260926])
    parser.add_argument("--seconds", type=int, default=5)
    parser.add_argument("--slo-ms", type=float, required=True, help="declared synthetic test budget, not a known HyperFeed SLA")
    parser.add_argument("--families", type=int, default=16)
    parser.add_argument("--hot-percent", type=int, default=80)
    parser.add_argument("--max-backlog", type=int, default=1000)
    parser.add_argument("--max-messages", type=int, default=100000)
    parser.add_argument("--max-worker-budget", type=int, default=32)
    parser.add_argument("--cpu-budget", type=int, default=len(os.sched_getaffinity(0)))
    parser.add_argument("--shm-mib", type=int, default=256)
    parser.add_argument("--pg-write-mode", choices=["buffered", "immediate"], default="buffered")
    parser.add_argument("--rpc-delay-us", type=int, default=0)
    parser.add_argument("--evidence", choices=["full", "metrics"], default="metrics")
    parser.add_argument("--correctness-report", type=Path)
    parser.add_argument("--timeout-seconds", type=float, help="per trial; default seconds + 420")
    parser.add_argument("--max-noop-fraction", type=float, default=0.25)
    parser.add_argument("--minimum-drain-fraction", type=float, default=0.95)
    parser.add_argument("--outcome-tolerance", type=float, default=0.10)
    args = parser.parse_args(arguments)
    try:
        dispatch_config(vars(args))
        maintenance_config(vars(args))
    except ValueError as error:
        parser.error(str(error))
    if args.workload != "calibrated" and any(getattr(args, field) != value for field, value in CALIBRATED_DEFAULTS.items()):
        parser.error("dispatch/signature/maintenance overrides apply only to --workload calibrated")
    engines = args.engines.split(",")
    if (not engines or len(set(engines)) != len(engines) or any(e not in ENGINES for e in engines)
            or not 1 <= min(args.rates) <= max(args.rates) <= 1000000
            or not 1 <= min(args.workers) <= max(args.workers) <= args.max_worker_budget <= 32
            or not 1 <= args.seconds <= 3600 or not 1 <= args.families <= 1024
            or (args.workload == "fleet" and (args.families < 16 or args.hot_percent != 0))
            or (args.workload == "calibrated" and (args.families < 4 or args.hot_percent != 0))
            or not 1 <= args.projection_interval_seconds <= 3600
            or not 1 <= args.housekeeping_interval_seconds <= 3600
            or (args.workload != "calibrated" and (args.projection_interval_seconds != 300 or args.housekeeping_interval_seconds != 600))
            or not 0 <= args.hot_percent <= 100 or args.cpu_budget < 1
            or not 1 <= args.max_backlog <= 100000 or not 1 <= args.max_messages <= 100000
            or not 32 <= args.shm_mib <= 3584 or not 0 <= args.rpc_delay_us <= 1000000
            or (args.rpc_delay_us and any(not engine.startswith("service-") for engine in engines))
            or not finite_number(args.slo_ms) or args.slo_ms <= 0
            or not 0 <= args.max_noop_fraction <= 1 or not 0 < args.minimum_drain_fraction <= 1
            or not 0 <= args.outcome_tolerance <= 1
            or (args.timeout_seconds is not None and (not finite_number(args.timeout_seconds) or args.timeout_seconds <= 0))
            or max(args.seeds) >= 2**64
            or (args.workload != "calibrated" and min(args.rates) * args.seconds < max(args.workers))
            or math.ceil(max(args.rates) * args.seconds / min(args.workers)) > args.max_messages):
        parser.error("invalid bounded matrix, resource budget, latency policy, or per-worker corpus size")
    if args.workload == "calibrated":
        for rate, workers in itertools.product(args.rates, args.workers):
            config = {"arrival_rate": rate, "workers": workers, "seconds": args.seconds,
                      "families": args.families, "hot_percent": args.hot_percent,
                      "projection_interval_seconds": args.projection_interval_seconds,
                      "housekeeping_interval_seconds": args.housekeeping_interval_seconds,
                      **{field: getattr(args, field) for field in CALIBRATED_DEFAULTS}}
            if max(calibrated_corpus(config)["worker_counts"]) > args.max_messages:
                parser.error("calibrated dispatch or timer corpus exceeds a per-process message cap")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    report_path = output / "campaign.json"
    # Immediately invalidate an old success, including before binary/PG setup.
    campaign = {"format": FORMAT, "completed": False, "passed": False, "source_stable": False,
                "started_at": datetime.now(timezone.utc).isoformat(), "trials": [],
                "whole_engine_verified": False, "actual_hyperfeed_replacement_qualified": False,
                "architecture_promotion_eligible": False, "exploratory": True}
    atomic_json(report_path, campaign)
    try:
        binary = args.binary.resolve(strict=True)
        secret = os.environ.get(args.pg_url_env, "")
        if "postgres" in engines and not secret:
            raise ValueError(f"PostgreSQL requires the named environment variable {args.pg_url_env}")
        password = unquote(urlsplit(secret).password or "") if "://" in secret else next((token.split("=", 1)[1] for token in shlex.split(secret) if token.startswith("password=")), "")
        secrets = tuple(value for value in (secret, password) if value)
        source = snapshot_sources()
        binary_sha = sha256(binary)
        policy = {name: getattr(args, name) for name in ("slo_ms", "max_noop_fraction", "minimum_drain_fraction", "outcome_tolerance")}
        campaign.update(source_before=source, binary_before_sha256=binary_sha, binary_path=str(binary),
                        compiler=subprocess.check_output(["rustc", "-Vv"], cwd=ROOT, text=True, timeout=10).strip(),
                        compiler_metadata_scope="current rustc -Vv; driver does not attest the compiler or sources used to build a caller-supplied binary",
                        binary_build_provenance="caller supplies built binary; hashes identify measured bytes but do not reconstruct its build",
                        git_revision=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, timeout=10).strip(),
                        host_before=host_info(), policy=policy,
                        matrix={"engines": engines, "rates": args.rates, "workers": args.workers, "seeds": args.seeds, "seconds": args.seconds},
                        resource_budget={"maximum_workers": args.max_worker_budget, "declared_cpus": args.cpu_budget,
                                         "native_shm_mib": args.shm_mib, "affinity_enforced": False,
                                         "note": "declared budget only; shared-host interference and PostgreSQL resource equivalence are not controlled"})
        if args.workload == "calibrated":
            campaign["resource_budget"].update(maximum_foreground_workers=args.max_worker_budget,
                                               maintenance_processes_per_trial=2,
                                               maximum_process_workers=args.max_worker_budget + 2,
                                               note="maximum_workers denotes foreground workers; calibrated trials add two maintenance processes; budgets are declared, not enforced, and do not establish equal engine resource use")
            campaign["calibrated_scope"] = {
                "foreground_ordering": "stable_foreground_worker_fifo" if args.dispatch == "identity" else "signature_affinity_worker_fifo",
                **{field: getattr(args, field) for field in CALIBRATED_DEFAULTS},
                "projection_interval_seconds": args.projection_interval_seconds,
                "housekeeping_interval_seconds": args.housekeeping_interval_seconds,
                "population_turnover_tested": False, "global_maintenance_sweep_complete": False,
                "capacity_qualification_available": False,
            }
        companion = None
        if args.correctness_report:
            companion = json.loads(args.correctness_report.read_text())
            campaign["correctness_report"] = {"path": str(args.correctness_report.resolve()), "sha256": sha256(args.correctness_report)}
        companion_ok = compatible_companion(companion, source, binary_sha)
        campaign["correctness_companion_provenance_compatible"] = companion_ok
        companion_keys = set()
        if companion_ok:
            for trial in companion.get("trials", []):
                if (trial.get("source_before_sha256") == source["sha256"] == trial.get("source_after_sha256")
                        and trial.get("binary_before_sha256") == binary_sha == trial.get("binary_after_sha256")
                        and assess_trial(trial, policy)["history_verified"]):
                    companion_keys.add(key(trial["config"]))
        atomic_json(report_path, campaign)
        for index, (seed, rate, workers) in enumerate(itertools.product(args.seeds, args.rates, args.workers)):
            for engine in engines if index % 2 == 0 else reversed(engines):
                directory = Path(tempfile.mkdtemp(prefix=f"{engine}-r{rate}-w{workers}-s{seed}-", dir=output))
                ownership_token = uuid.uuid4().hex
                (directory / ".qualification-owned-trial").write_text(ownership_token)
                config = {"engine": engine, "workload": args.workload, "evidence": args.evidence,
                          "families": args.families, "hot_percent": args.hot_percent, "seed": seed,
                          "arrival_rate": rate, "seconds": args.seconds, "workers": workers,
                          "pg_write_mode": args.pg_write_mode, "rpc_delay_us": args.rpc_delay_us,
                          "global_time_predicates": False, "shm_mib": args.shm_mib,
                          "max_backlog": args.max_backlog, "max_messages": args.max_messages,
                          "message_interval_us": 0, "expiry_index_policy": args.expiry_index_policy,
                          "retry_diagnostics": args.retry_diagnostics == "on"}
                if args.workload == "calibrated":
                    config.update(projection_interval_seconds=args.projection_interval_seconds,
                                  housekeeping_interval_seconds=args.housekeeping_interval_seconds,
                                  **{field: getattr(args, field) for field in CALIBRATED_DEFAULTS})
                command = [str(binary), "--mode", "sustained", "--output", str(directory / "report.json")]
                for field in ("engine", "workload", "evidence", "families", "hot_percent", "seed", "arrival_rate", "seconds", "workers", "pg_write_mode", "rpc_delay_us"):
                    command += ["--" + field.replace("_", "-"), str(config[field])]
                command += ["--max-backlog", str(args.max_backlog), "--max-messages", str(args.max_messages), "--shm-mib", str(args.shm_mib)]
                if args.workload == "calibrated":
                    for name in CALIBRATED_FIELDS + tuple(CALIBRATED_DEFAULTS):
                        command += ["--" + name.replace("_", "-"), str(config[name])]
                command += ["--expiry-index", args.expiry_index_policy, "--retry-diagnostics", args.retry_diagnostics]
                public_command = command.copy()
                if engine == "postgres":
                    command += ["--pg-url", secret]
                    public_command += ["--pg-url", "<environment:" + args.pg_url_env + ">"]
                before = snapshot_sources()
                before_binary = sha256(binary)
                trial = {"config": config, "directory": str(directory), "command": public_command,
                         "source_before_sha256": before["sha256"], "binary_before_sha256": before_binary}
                campaign["trials"].append(trial)
                atomic_json(report_path, campaign)
                print(f"START {engine} rate={rate} workers={workers} seed={seed}", flush=True)
                try:
                    trial.update(run_process(command, directory / "run.log", args.timeout_seconds or args.seconds + 420, secrets,
                                             directory, ownership_token))
                    raw = (directory / "report.json").read_text()
                    for value in secrets:
                        raw = raw.replace(value, "<redacted>")
                    (directory / "report.json").write_text(raw)
                    trial["report"] = json.loads(raw)
                except Exception as error:
                    trial["error"] = type(error).__name__ + ": " + str(error)
                    for value in secrets:
                        trial["error"] = trial["error"].replace(value, "<redacted>")
                after = snapshot_sources()
                after_binary = sha256(binary)
                trial.update(source_after_sha256=after["sha256"], binary_after_sha256=after_binary,
                             source_stable=source == before == after and binary_sha == before_binary == after_binary)
                trial["assessment"] = assess_trial(trial, policy, companion_keys)
                atomic_json(directory / "trial.json", trial)
                atomic_json(report_path, campaign)
                print(f"DONE {engine} valid={trial['assessment']['execution_valid']} capacity_pass={trial['assessment']['qualified_capacity_trial']}", flush=True)
        campaign.update(source_after=snapshot_sources(), binary_after_sha256=sha256(binary), host_after=host_info(), completed=True)
        campaign["source_stable"] = campaign["source_before"] == campaign["source_after"] and binary_sha == campaign["binary_after_sha256"] and all(t["source_stable"] for t in campaign["trials"])
        if not campaign["source_stable"]:
            for trial in campaign["trials"]:
                trial["source_stable"] = False
        campaign["gate"] = build_gate(campaign["trials"], policy, engines, args.rates, args.workers, args.seeds, companion_keys)
        campaign["passed"] = campaign["source_stable"] and all(t["assessment"]["execution_valid"] for t in campaign["gate"]["trial_assessments"])
    except Exception as error:
        campaign["operational_error"] = type(error).__name__ + ": " + str(error)
    finally:
        atomic_json(report_path, campaign)
    return 0 if campaign["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
