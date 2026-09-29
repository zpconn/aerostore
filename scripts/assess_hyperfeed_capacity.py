#!/usr/bin/env python3
"""Assess sustained capacity of explicitly declared synthetic configurations.

This additive assessor does not change qualify_hyperfeed's replacement gates.
A long metrics run has structural checks and a source-bound short full-history
safety guardrail, not a proof of its own unrecorded history. Failed configurations
remain operational failures; resource, generator and oracle limits are censored.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import re
from statistics import mean
import sys

import qualify_hyperfeed as gate

GIB = 1024 ** 3
FORMAT = "hyperfeed-capacity-assessment-v1"
INPUT_FORMAT = "hyperfeed-capacity-input-v1"
DEFAULT_POLICY = {
    "minimum_duration_seconds": 905, "warmup_seconds": 60,
    "foreground_p99_ms": 50, "minimum_completed_fraction": .99,
    "minimum_final_third_completed_fraction": .99,
    "max_queue_seconds": 1, "max_queue_growth_seconds": .05,
    "max_oldest_foreground_age_seconds": 1,
    "minimum_completed_maintenance_jobs": 3, "minimum_positive_maintenance_jobs": 2,
    "required_repeats": 2, "memory_limit_bytes": 36 * GIB,
    "host_reserve_bytes": 4 * GIB,
}
GUARDRAIL_DIFFERENCES = {"seconds", "seed", "projection_interval_seconds", "housekeeping_interval_seconds"}


def number(value, name, minimum=0):
    if not gate.finite_number(value) or value < minimum:
        raise ValueError(f"{name} must be finite and >= {minimum}")
    return value


def integer(value, name, minimum=0):
    if type(value) is not int or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def policy_with_defaults(policy=None):
    policy = policy or {}
    if set(policy) - set(DEFAULT_POLICY):
        raise ValueError("unknown capacity policy field")
    result = {**DEFAULT_POLICY, **policy}
    for key, value in result.items():
        number(value, key)
    for key in ("minimum_duration_seconds", "warmup_seconds", "minimum_completed_maintenance_jobs",
                "minimum_positive_maintenance_jobs", "required_repeats", "memory_limit_bytes", "host_reserve_bytes"):
        integer(result[key], key, 1 if key != "warmup_seconds" else 0)
    if result["warmup_seconds"] >= result["minimum_duration_seconds"]:
        raise ValueError("warmup must end before the minimum duration")
    for key in ("minimum_completed_fraction", "minimum_final_third_completed_fraction"):
        if not 0 < result[key] <= 1:
            raise ValueError("completion fractions must lie in (0, 1]")
    return result


def trend(points):
    """Describe, not automatically reject, growth in a named measurement."""
    if len(points) < 2:
        raise ValueError("a memory trend needs at least two samples")
    for x, y in points:
        number(x, "sample time"); number(y, "sample value")
    if any(b[0] <= a[0] for a, b in zip(points, points[1:])):
        raise ValueError("sample times must increase")
    xmean, ymean = mean(x for x, _ in points), mean(y for _, y in points)
    slope = sum((x-xmean)*(y-ymean) for x, y in points) / sum((x-xmean)**2 for x, _ in points)
    return dict(samples=len(points), first=points[0][1], last=points[-1][1],
                minimum=min(y for _, y in points), maximum=max(y for _, y in points),
                elapsed_seconds=points[-1][0]-points[0][0], slope_per_second=slope,
                maximum_sample_gap_seconds=max(b[0]-a[0] for a, b in zip(points, points[1:])))


def counters(text):
    if not isinstance(text, str):
        raise ValueError("kernel counters must be text")
    pairs = [line.split() for line in text.splitlines() if line.strip()]
    result = {key: int(value) for key, value in pairs}
    if len(result) != len(pairs) or any(value < 0 for value in result.values()):
        raise ValueError("duplicate or negative kernel counter")
    return result


def resource_assessment(envelope, readiness, samples, policy):
    reasons = []
    if not (envelope.get("ready") is True and envelope.get("controller_survived") is True
            and envelope.get("controller_boot_before") == envelope.get("controller_boot_after") == readiness.get("boot_id")
            and readiness.get("ready") is True):
        reasons.append("resource controller/readiness/boot continuity failed")
    if envelope.get("reason") != "unit-completed":
        reasons.append("resource controller interrupted the workload: " + str(envelope.get("reason")))
    if envelope.get("cleanup", {}).get("owned_processes_terminated") is not True:
        reasons.append("owned processes were not confirmed stopped")
    if readiness.get("memory_max_bytes") != policy["memory_limit_bytes"] or readiness.get("reserve_bytes") != policy["host_reserve_bytes"]:
        reasons.append("resource limits differ from the declared policy")
    if envelope.get("memory_events_scope") != "unique-accounting-slice":
        raise ValueError("resource accounting scope is not the owned slice")
    final = envelope["final_accounting"]
    peak = integer(int(final["memory.peak"]), "memory peak")
    host_min = integer(envelope["minimum_mem_available_bytes"], "minimum available host memory")
    if peak > policy["memory_limit_bytes"] or host_min < policy["host_reserve_bytes"]:
        reasons.append("memory cap or host reserve violated")
    for key in ("memory.events", "memory.events.local"):
        values = counters(final[key])
        if any(values.get(name) != 0 for name in ("high", "max", "oom", "oom_kill", "oom_group_kill")):
            reasons.append("nonzero or missing memory pressure/OOM counters")
    if int(final["memory.swap.current"]) != 0 or int(final["memory.swap.peak"]) != 0:
        reasons.append("swap occurred")
    if not isinstance(samples, list) or len(samples) < 2:
        raise ValueError("missing memory samples")
    if envelope.get("samples") != len(samples):
        raise ValueError("memory sample count differs from controller receipt")
    points = {key: [] for key in ("current", "anon", "file", "shmem")}
    for sample in samples:
        elapsed = number(sample["elapsed_seconds"], "resource elapsed")
        stat = counters(sample["memory.stat"])
        values = {"current": int(sample["memory.current"]), **{key: stat[key] for key in ("anon", "file", "shmem")}}
        for key, value in values.items():
            points[key].append((elapsed, integer(value, key)))
        if values["current"] > policy["memory_limit_bytes"] or sample["mem_available_bytes"] < policy["host_reserve_bytes"]:
            reasons.append("sampled memory cap or host reserve violated")
        if int(sample["memory.swap.current"]) or int(sample["memory.swap.peak"]):
            reasons.append("sampled swap occurred")
    return {"passed": not reasons, "reasons": sorted(set(reasons)), "peak_bytes": peak,
            "minimum_host_available_bytes": host_min, "trends": {key: trend(value) for key, value in points.items()},
            "trend_scope": "whole owned envelope, including setup, teardown, checking and coordinator retention; not isolated database RSS",
            "flat_memory_required": False}


def source_identity(campaign):
    if campaign.get("format") != gate.FORMAT or campaign.get("source_stable") is not True:
        raise ValueError("campaign source identity is missing or unstable")
    before, after = campaign.get("source_before"), campaign.get("source_after")
    if not isinstance(before, dict) or before != after:
        raise ValueError("campaign source snapshots differ")
    source, binary = before.get("sha256"), campaign.get("binary_before_sha256")
    if not all(isinstance(x, str) and re.fullmatch(r"[0-9a-f]{64}", x) for x in (source, binary)):
        raise ValueError("malformed source or binary fingerprint")
    if binary != campaign.get("binary_after_sha256"):
        raise ValueError("binary changed during campaign")
    return source, binary


def error_classification(trial, failure=None):
    runs = trial.get("report", {}).get("runs", [])
    text = "\n".join(str(x) for x in (trial.get("error", ""), trial.get("report", {}).get("error", ""),
                                           (failure or {}).get("error", ""), *(run.get("error", "") for run in runs)))
    states = {run.get("oracle_status") for run in runs}
    if "Invalid" in states or any(run.get("invariants", {}).get("passed") is False for run in runs):
        return "correctness_failure", "history or structural correctness failed", text
    if "Inconclusive" in states or re.search(r"oracle.*(budget|inconclusive)", text, re.I):
        return "censored_oracle", "oracle budget or inconclusive history", text
    if re.search(r"(message_cap|max.messages|corpus.*(cap|bound)|generator.*(bound|limit))", text, re.I):
        return "censored_generator", "generator/corpus bound reached", text
    # workers::execute wraps fatal/transport errors with the same prose. Only
    # the source-bound 128-retry Conflict terminal is exhaustion evidence.
    if re.search(r"after 128 retries: transaction conflict(?:;|\n|$)", text):
        return "operational_failure", "retry_exhaustion", text
    if re.search(r"backlog.*(exceeds|bound)", text, re.I):
        return "operational_failure", "backlog_bound_exceeded", text
    if re.search(r"(maintenance|sweep).*(terminal|cap|batches|deadline)", text, re.I):
        return "operational_failure", "maintenance_starvation", text
    if "no worker progress for 60 seconds" in text:
        return "operational_failure", "worker_progress_stall", text
    if trial.get("timed_out"):
        return "censored_timeout", "timeout stage is not independently established", text
    if trial.get("exit_code") != 0 or trial.get("report", {}).get("passed") is not True:
        return "censored_operational", "unclassified incomplete execution", text
    return None, None, text


def guardrail_message_cap_exception(config, other):
    """Prove that differing resource ceilings cannot truncate either corpus.

    This applies only to the capacity assessor's shorter full-history guardrail.
    Exact qualifier companions and repeat configuration keys retain max_messages.
    Counts include exact dispatched foreground owners and both timer workers;
    maintenance transaction batches are not separate generated worker inputs.
    """
    if (config.get("workload") != "calibrated" or other.get("workload") != "calibrated"
            or config.get("evidence") != "metrics" or other.get("evidence") != "full"):
        raise ValueError("differing message caps require calibrated metrics and full guardrail")
    proof = {}
    for label, settings, ceiling in (("measured", config, 1_000_000), ("guardrail", other, 100_000)):
        cap = integer(settings.get("max_messages"), label+" max_messages", 1)
        if cap > ceiling:
            raise ValueError(label+" max_messages exceeds its evidence-mode bound")
        corpus = gate.calibrated_corpus(settings)
        counts = corpus["worker_counts"]
        if len(counts) != settings["workers"] + 2 or any(type(n) is not int or n < 0 for n in counts):
            raise ValueError(label+" exact generated worker counts are invalid")
        if max(counts) > cap:
            raise ValueError(label+" exact dispatched or timer corpus exceeds max_messages")
        proof[label] = {"max_messages": cap, "worker_counts": counts,
                        "maximum_worker_inputs": max(counts),
                        "foreground_inputs": corpus["foreground"],
                        "projection_inputs": corpus["projection"],
                        "housekeeping_inputs": corpus["housekeeping"],
                        "total_inputs": corpus["total"]}
    return {"passed": True, **proof,
            "scope": "independent exact generated input counts fit each resource cap; no workload or transaction difference is excused"}


def guardrail_assessment(trial, campaign, guardrail_trial, guardrail_campaign, policy, failure=False):
    identity = source_identity(campaign)
    if source_identity(guardrail_campaign) != identity:
        raise ValueError("guardrail source/binary differs")
    gp = {"slo_ms": policy["foreground_p99_ms"], "minimum_drain_fraction": .95,
          "max_noop_fraction": 0, "outcome_tolerance": 0}
    assessment = gate.assess_trial(guardrail_trial, gp)
    if not (assessment["execution_valid"] and assessment["history_verified"] and assessment.get("useful_work_passed")):
        raise ValueError("short guardrail lacks a valid useful complete history")
    config, other = trial["config"], guardrail_trial["config"]
    differences = {}
    for field in set(gate.match_fields(config)) | set(gate.match_fields(other)):
        a, b = gate.config_value(config, field), gate.config_value(other, field)
        if type(a) is not type(b) or a != b:
            differences[field] = {"measured": a, "guardrail": b}
    allowed = GUARDRAIL_DIFFERENCES | ({"arrival_rate"} if failure else set())
    cap_proof = None
    if "max_messages" in differences:
        cap_proof = guardrail_message_cap_exception(config, other)
        allowed |= {"max_messages"}
    if set(differences) - allowed:
        raise ValueError("guardrail changes transaction/workload settings beyond declared short-run differences")
    if failure and other["arrival_rate"] > config["arrival_rate"]:
        raise ValueError("failure guardrail must be at the same or a lower rate")
    return {"passed": True, "differences": differences, "nonbinding_message_cap_difference": cap_proof,
            "guardrail_history_verified": True,
            "measured_run_history_verified": False,
            "scope": "structural checks plus same-build short full-history guardrail; not verification of the long run's unrecorded history"}


def storage_assessment(run, engine):
    samples = run.get("retention_samples")
    if not isinstance(samples, list) or len(samples) < 2:
        raise ValueError("missing per-run storage trend")
    native = engine != "postgres"
    keys = ("arena_high_water_bytes", "row_fresh_allocations", "row_reuse_allocations", "row_recycled") if native else ("table_bytes", "index_bytes", "total_bytes", "estimated_dead_tuples")
    points = {key: [] for key in keys}
    retired, errors = [], []
    for sample in samples:
        at = number(sample["elapsed_seconds"], "storage elapsed")
        storage = sample["storage"]
        for key in keys:
            points[key].append((at, number(storage[key], key)))
        if native:
            indexes = storage["indexes"]
            retired.append((at, sum(integer(x["retired_postings"], "retired postings") for x in indexes)))
            if any(x.get("alloc_failures") != 0 or x.get("gc_recycle_errors") != 0 for x in indexes):
                errors.append("native index allocation or recycle errors")
    after = run.get("after_drain", {})
    if native:
        if not isinstance(run.get("native_audit"), dict) or not run["native_audit"].get("indexes"):
            raise ValueError("missing final native allocation/index audit")
        if after.get("active_transactions") != 0 or not after.get("indexes"):
            raise ValueError("missing clean drained native storage snapshot")
        if any(x.get("alloc_failures") != 0 or x.get("gc_recycle_errors") != 0 for x in after["indexes"]):
            errors.append("native final allocation or recycle errors")
    summary = {"passed": not errors, "reasons": sorted(set(errors)), "trends": {key: trend(value) for key, value in points.items()},
               "scope": "native arena high-water/reuse/retired counters; not RSS" if native else "PostgreSQL relation bytes and estimated dead tuples; not RSS"}
    if native:
        remaining = sum(integer(x["retired_postings"], "final retired postings") for x in after["indexes"])
        summary.update(retired_postings_trend=trend(retired), retired_postings_after_drain=remaining,
                       retired_backlog_observed_after_drain=remaining > 0,
                       retired_backlog_is_automatic_failure=False)
    return summary


def accounting_assessment(accounting, config, run, policy, partial=False):
    if accounting.get("format") != "capacity-accounting-v1" or accounting.get("completion_clock") != "coordinator_received_ns":
        raise ValueError("missing supported receipt-based capacity accounting")
    start = integer(accounting["admission_start_ns"], "admission start")
    end = integer(accounting["admission_end_ns"], "admission end", start + 1)
    seconds = integer(config["seconds"], "duration", 1)
    rate = integer(config["arrival_rate"], "offered rate", 1)
    if end-start != seconds * 1_000_000_000:
        raise ValueError("accounting admission duration differs from config")
    observed = integer(accounting["observed_until_ns"], "observation clock")
    if not partial and (accounting.get("execution_completed") is not True
                        or accounting.get("admission_fully_observed") is not True or observed < end):
        raise ValueError("completed run lacks complete admission observation")
    fg = accounting["foreground"]
    offered = integer(fg["offered"], "foreground offered", 1)
    if offered != rate * seconds:
        raise ValueError("foreground offer is not one count per offered input")
    received = integer(fg["received_in_admission"], "foreground received")
    total = integer(fg["completed_total"], "foreground total")
    if not received <= total <= offered:
        raise ValueError("foreground completion totals are inconsistent")
    if not partial:
        if (total != offered or fg.get("remaining_at_admission_end") != offered-received
                or fg.get("in_admission_counts_final") is not True):
            raise ValueError("foreground admission/drain totals are incomplete")
        if not math.isclose(number(fg["completion_rate_in_admission"], "in-admission rate"), received/seconds, rel_tol=1e-9):
            raise ValueError("in-admission rate has the wrong denominator")
        classes = run["workload_classes"]["foreground"]
        if classes["completed"] != total or classes["offered"] != offered:
            raise ValueError("foreground logical counts differ from workload classes")
        if not math.isclose(number(fg["p99_us_including_retries"], "foreground p99"),
                            number(classes["p99_us_including_retries"], "class p99"), rel_tol=1e-9):
            raise ValueError("foreground p99 differs from retry-inclusive class metric")
    windows = accounting["windows"]
    if accounting.get("window_ns") != 1_000_000_000 or not isinstance(windows, list):
        raise ValueError("missing one-second foreground windows")
    previous_end, cumulative_arrivals, cumulative_completions = 0, 0, 0
    complete = []
    for window in windows:
        a = integer(window["start_offset_ns"], "window start")
        b = integer(window["end_offset_ns"], "window end", a+1)
        cutoff = integer(window["observed_end_offset_ns"], "window observation", a)
        if a != previous_end or b != min(a+1_000_000_000, end-start) or not a <= cutoff <= b:
            raise ValueError("capacity windows have gaps, overlap or invalid bounds")
        if a >= max(0, observed-start) or cutoff != min(b, max(0, observed-start)):
            raise ValueError("capacity window extends beyond its observation")
        arrived = integer(window["arrivals"], "window arrivals")
        done = integer(window["completions"], "window completions")
        if window["backlog_start"] != cumulative_arrivals-cumulative_completions:
            raise ValueError("window starting backlog differs from receipt counts")
        cumulative_arrivals += arrived; cumulative_completions += done
        if (cumulative_completions > cumulative_arrivals
                or window["cumulative_arrivals"] != cumulative_arrivals
                or window["cumulative_completions"] != cumulative_completions
                or window["backlog_end"] != cumulative_arrivals-cumulative_completions):
            raise ValueError("window queue is inconsistent with offered/completed inputs")
        expected_arrivals = (cutoff*rate + 999_999_999)//1_000_000_000 - (a*rate + 999_999_999)//1_000_000_000
        if arrived != expected_arrivals or type(window["fully_observed"]) is not bool or window["fully_observed"] != (cutoff == b):
            raise ValueError("window arrival coverage or observation flag is wrong")
        if window["backlog_end"]:
            integer(window["oldest_uncompleted_age_ns"], "oldest pending foreground age")
        elif window["oldest_uncompleted_age_ns"] is not None:
            raise ValueError("empty backlog has an outstanding age")
        planned = (b-a)*rate//1_000_000_000
        if window["planned_arrivals"] != planned or window["arrival_cohort_completed"] + window["arrival_cohort_unfinished"] != planned:
            raise ValueError("arrival cohort completeness differs from offered window")
        for key in ("arrival_cohort_completed", "arrival_cohort_unfinished", "completed_message_retries"):
            integer(window[key], key)
        if window["fully_observed"]:
            complete.append(window)
        previous_end = b
    expected_window_end = min(end-start, math.ceil(max(0, observed-start)/1_000_000_000)*1_000_000_000)
    if previous_end != expected_window_end:
        raise ValueError("observed capacity windows are missing")
    if not partial and (previous_end != end-start or cumulative_arrivals != offered
                        or cumulative_completions != received
                        or any(w["arrival_cohort_unfinished"] for w in windows)):
        raise ValueError("complete admission/arrival cohorts differ from windows")
    reasons, maintenance = [], {}
    original_jobs = {(job["class"], job["job_ordinal"]): job for job in run.get("maintenance_jobs", [])}
    for name in ("projection", "housekeeping"):
        info = accounting["maintenance"][name]
        interval = integer(config[name+"_interval_seconds"], name+" interval", 1)
        expected = (seconds-1)//interval
        jobs = info["jobs"]
        if info["offered"] != expected or len(jobs) != expected:
            raise ValueError("maintenance jobs differ from scheduled timer corpus")
        completed = positive = late = overdue = 0
        for ordinal, job in enumerate(jobs):
            scheduled, deadline = start+(ordinal+1)*interval*1_000_000_000, start+(ordinal+2)*interval*1_000_000_000
            if job["ordinal"] != ordinal or job["scheduled_ns"] != scheduled or job["deadline_ns"] != deadline:
                raise ValueError("maintenance deadline differs from its next cadence tick")
            if job["completed"]:
                when = integer(job["received_ns"], "maintenance received", scheduled)
                if when > observed:
                    raise ValueError("maintenance receipt is beyond observation")
                completed += 1; late += when >= deadline
                positive += job.get("positive_effect") is True
                if not partial:
                    original = original_jobs.get((name, ordinal))
                    if not original or any(job.get(key) != original.get(key)
                            for key in ("scheduled_ns", "started_ns", "finished_ns", "received_ns", "retries")):
                        raise ValueError("capacity maintenance job differs from complete sweep receipt")
                    if job.get("positive_effect") is not (original["processed_rows"] > 0):
                        raise ValueError("capacity maintenance effect differs from complete sweep receipt")
            else:
                overdue += observed >= deadline
        if info["completed"] != completed or info.get("positive_effect_jobs") != positive:
            raise ValueError("maintenance aggregate differs from its jobs")
        if late or overdue:
            reasons.append(name+" missed its next-tick deadline")
        if not partial and (completed != expected or completed < policy["minimum_completed_maintenance_jobs"]):
            reasons.append(name+" did not complete the required whole jobs")
        if not partial and positive < policy["minimum_positive_maintenance_jobs"]:
            reasons.append(name+" lacks repeated positive work")
        maintenance[name] = dict(offered=expected, completed=completed, positive=positive,
                                 late_completed=late, unfinished_past_deadline=overdue)
    result = dict(foreground_offered=offered, foreground_received_in_admission=received,
                  foreground_completed_including_drain=total, completion_fraction=received/offered,
                  maintenance=maintenance, reasons=reasons,
                  count_scope="logical foreground inputs once; maintenance batches, retries and fork writes excluded")
    if partial:
        return result
    duration = end-start
    post = [w for w in complete if w["start_offset_ns"] >= policy["warmup_seconds"]*1_000_000_000]
    late_windows = [w for w in complete if 3*w["start_offset_ns"] >= 2*duration]
    post_start = policy["warmup_seconds"]*1_000_000_000
    early = [w for w in post if 3*(w["end_offset_ns"]-post_start) <= duration-post_start]
    late = [w for w in post if 3*(w["start_offset_ns"]-post_start) >= 2*(duration-post_start)]
    if not early or not late or not late_windows:
        raise ValueError("insufficient complete windows for queue and final-third assessment")
    late_arrivals = sum(w["arrivals"] for w in late_windows)
    late_received = sum(w["completions"] for w in late_windows)
    queue_max = max(w["backlog_end"] for w in post)
    queue_growth = mean(w["backlog_end"] for w in late) - mean(w["backlog_end"] for w in early)
    oldest = max((w["oldest_uncompleted_age_ns"] or 0) for w in post)
    p99 = number(fg["p99_us_including_retries"], "foreground p99") / 1000
    if received/offered < policy["minimum_completed_fraction"]:
        reasons.append("foreground in-admission completion fraction below policy")
    if late_received/late_arrivals < policy["minimum_final_third_completed_fraction"]:
        reasons.append("final-third foreground completion fraction below policy")
    if p99 > policy["foreground_p99_ms"]:
        reasons.append("foreground p99 including queue/retries exceeds policy")
    if queue_max > math.ceil(rate*policy["max_queue_seconds"]):
        reasons.append("sampled foreground backlog exceeds policy")
    if queue_growth > math.ceil(rate*policy["max_queue_growth_seconds"]):
        reasons.append("sampled foreground backlog grows beyond policy")
    if oldest > policy["max_oldest_foreground_age_seconds"]*1e9:
        reasons.append("sampled oldest outstanding foreground age exceeds policy")
    result.update(foreground_p99_ms=p99, final_third=dict(completions=late_received, offered=late_arrivals,
        fraction=late_received/late_arrivals, start_offset_seconds=late_windows[0]["start_offset_ns"]/1e9,
        duration_seconds=sum(w["end_offset_ns"]-w["start_offset_ns"] for w in late_windows)/1e9),
        queue=dict(sampled_maximum=queue_max, late_minus_early_mean=queue_growth,
                   sampled_oldest_age_seconds=oldest/1e9,
                   trend=trend([(w["end_offset_ns"]/1e9,w["backlog_end"]) for w in post]),
                   scope="one-second receipt-based observations after warmup; not an exact continuous maximum"))
    return result


def assess_trial(evidence, policy=None):
    policy = policy_with_defaults(policy)
    trial, campaign = evidence["trial"], evidence["campaign"]
    config = trial.get("config", {})
    result = {"classification": "invalid_evidence", "conditional_capacity_passed": False,
              "operational_capacity_failure": False, "censored": False, "reasons": [],
              "config": config, "actual_hyperfeed_replacement_qualified": False,
              "architecture_promotion_eligible": False, "whole_engine_verified": False}
    try:
        source, binary = source_identity(campaign)
        result.update(source_sha256=source, binary_sha256=binary)
        resources = resource_assessment(evidence["envelope"], evidence["readiness"], evidence["memory_samples"], policy)
        result["resources"] = resources
        if not resources["passed"]:
            result.update(classification="censored_resources", censored=True, reasons=resources["reasons"])
            return result
        category, reason, error = error_classification(trial, evidence.get("failure_progress"))
        result["error_text"] = error
        partial = evidence.get("capacity_accounting") or (evidence.get("failure_progress") or {}).get("capacity_accounting")
        if category == "censored_timeout" and partial:
            observed = accounting_assessment(partial, config, {}, policy, partial=True)
            if any(value["unfinished_past_deadline"] for value in observed["maintenance"].values()):
                category, reason = "operational_failure", "maintenance_starvation_observed_before_timeout"
        if category and category != "operational_failure":
            result.update(classification=category, censored=category.startswith("censored"), reasons=[reason])
            return result
        result["correctness_coverage"] = guardrail_assessment(trial, campaign, evidence["guardrail_trial"],
            evidence["guardrail_campaign"], policy, failure=category == "operational_failure")
        if category == "operational_failure":
            result.update(classification=category, operational_capacity_failure=True, reasons=[reason])
            if partial:
                result["accounting"] = accounting_assessment(partial, config, {}, policy, partial=True)
            if reason == "worker_progress_stall" and (not partial or partial["foreground"].get("offered_observed", 0) <= 0):
                result.update(classification="censored_timeout", censored=True,
                    operational_capacity_failure=False, reasons=["stall lacks observed positive foreground arrivals"])
            return result
        if (config.get("workload") != "calibrated" or gate.config_value(config, "maintenance_mode") != "sweep"
                or gate.config_value(config, "rolling_cycle_messages") != 0):
            raise ValueError("this policy assesses the fixed-population calibrated sweep configuration")
        if config.get("seconds", 0) < policy["minimum_duration_seconds"]:
            result.update(classification="censored_duration", censored=True, reasons=["screen duration is shorter than sustained policy"])
            return result
        check = gate.assess_trial(trial, {"slo_ms": policy["foreground_p99_ms"], "minimum_drain_fraction": .95,
            "max_noop_fraction": 0, "outcome_tolerance": 0})
        result["structural_assessment"] = {key: check.get(key) for key in ("execution_valid", "continuous_timing_passed",
            "useful_work_passed", "history_verified", "global_maintenance_sweep_complete")}
        if not all(check.get(key) is True for key in ("execution_valid", "continuous_timing_passed", "useful_work_passed", "global_maintenance_sweep_complete")):
            raise ValueError("execution/structural/useful-work checks failed: " + "; ".join(check["reasons"]))
        run = trial["report"]["runs"][0]
        storage = storage_assessment(run, config["engine"])
        result["storage"] = storage
        if not storage["passed"]:
            result.update(classification="operational_failure", operational_capacity_failure=True, reasons=storage["reasons"])
            return result
        measured = accounting_assessment(run["capacity_accounting"], config, run, policy)
        result["accounting"] = measured
        result["reasons"] = measured["reasons"]
        result.update(classification="completed_policy_failure" if measured["reasons"] else "passed",
                      conditional_capacity_passed=not measured["reasons"],
                      operational_capacity_failure=bool(measured["reasons"]))
    except (KeyError, TypeError, ValueError, ZeroDivisionError, OverflowError) as error:
        result.update(classification="invalid_evidence", reasons=[str(error)],
                      conditional_capacity_passed=False, operational_capacity_failure=False, censored=False)
    return result


def summarize_capacity(assessments, policy=None):
    policy = policy_with_defaults(policy)
    groups = {}
    for row in assessments:
        if "source_sha256" not in row or "binary_sha256" not in row:
            continue
        config = row["config"]
        base = {name: gate.config_value(config, name) for name in gate.match_fields(config)
                if name not in ("arrival_rate", "seed")}
        grouping = json.dumps([row["source_sha256"], row["binary_sha256"], base], sort_keys=True)
        group = groups.setdefault(grouping, {"configuration": base, "source_sha256": row["source_sha256"],
            "binary_sha256": row["binary_sha256"], "rates": {}})
        group["rates"].setdefault(config["arrival_rate"], []).append(row)
    output = []
    for group in groups.values():
        rates, repeated_passes, repeated_failures = [], [], []
        for rate, rows in sorted(group.pop("rates").items()):
            seeds = [row["config"]["seed"] for row in rows]
            enough = len(set(seeds)) >= policy["required_repeats"]
            success = enough and all(row["classification"] == "passed" and row["conditional_capacity_passed"] for row in rows)
            failure = enough and all(row["classification"] in {"operational_failure", "completed_policy_failure"}
                                     and row["operational_capacity_failure"] for row in rows)
            if success: repeated_passes.append(rate)
            if failure: repeated_failures.append(rate)
            rates.append(dict(offered_rate=rate, attempts=len(rows), distinct_seeds=sorted(set(seeds)),
                classifications=[row["classification"] for row in rows], repeated_pass=success, repeated_failure=failure,
                variation_scope="seeds change synthetic coordinates, not deterministic arrival/dispatch order"))
        lower = max(repeated_passes, default=None)
        failed_above = min((rate for rate in repeated_failures if lower is not None and rate > lower), default=None)
        group.update(rates=rates, tested_capacity_lower_bound_inputs_per_second=lower,
            next_repeated_failed_offered_rate=failed_above,
            nonmonotonic_tested_points=any(fail < passed for fail in repeated_failures for passed in repeated_passes),
            capacity_lower_bound_established=lower is not None,
            universal_upper_bound_established=False,
            scope="Conditional synthetic configuration and resource budget. Repeated failed points delimit an operational bracket; they are not a theorem that every higher rate fails.")
        output.append(group)
    return {"configurations": output, "actual_hyperfeed_replacement_qualified": False,
            "real_world_10x_claim": False, "ratio_of_lower_bounds_is_capacity_ratio": False}


def load_artifact(reference, *, json_lines=False):
    if not isinstance(reference, dict) or set(reference) != {"path", "sha256"}:
        raise ValueError("artifact reference needs exactly path and sha256")
    path = Path(reference["path"]).resolve(strict=True)
    content = path.read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    if digest != reference["sha256"]:
        raise ValueError("artifact hash mismatch: " + str(path))
    value = [json.loads(line) for line in content.splitlines() if line.strip()] if json_lines else json.loads(content)
    return value, {"path": str(path), "sha256": digest}


def assess_manifest(manifest):
    if manifest.get("format") != INPUT_FORMAT:
        raise ValueError("unsupported capacity input manifest")
    policy = policy_with_defaults(manifest.get("policy"))
    rows, bindings, ids = [], [], set()
    for entry in manifest["trials"]:
        if not isinstance(entry.get("id"), str) or entry["id"] in ids:
            raise ValueError("trial IDs must be unique strings")
        ids.add(entry["id"])
        loaded = {}
        for name in ("campaign", "guardrail", "envelope", "readiness", "memory_samples"):
            loaded[name], bound = load_artifact(entry[name], json_lines=name == "memory_samples")
            bindings.append(bound)
        trial = loaded["campaign"]["trials"][integer(entry.get("trial_index", 0), "trial index")]
        guardrail = loaded["guardrail"]["trials"][integer(entry.get("guardrail_trial_index", 0), "guardrail trial index")]
        data = dict(trial=trial, campaign=loaded["campaign"], guardrail_trial=guardrail,
                    guardrail_campaign=loaded["guardrail"], envelope=loaded["envelope"],
                    readiness=loaded["readiness"], memory_samples=loaded["memory_samples"])
        if entry.get("failure_progress"):
            data["failure_progress"], bound = load_artifact(entry["failure_progress"])
            bindings.append(bound)
        if entry.get("capacity_accounting"):
            data["capacity_accounting"], bound = load_artifact(entry["capacity_accounting"])
            bindings.append(bound)
        result = assess_trial(data, policy); result["id"] = entry["id"]; rows.append(result)
    # Retain all failures. A valid negative assessment is not a successful run.
    return {"format": FORMAT, "policy": policy, "assessments": rows, "input_bindings": bindings,
            "all_trials_passed": bool(rows) and all(row["conditional_capacity_passed"] for row in rows),
            "assessment_complete": all(row["classification"] != "invalid_evidence" for row in rows),
            **summarize_capacity(rows, policy)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.exists():
        parser.error("refusing to replace a previous capacity assessment")
    receipt = {"format": FORMAT, "assessment_complete": False, "all_trials_passed": False}
    try:
        content = args.input.read_bytes()
        receipt.update(input_path=str(args.input.resolve()), input_sha256=hashlib.sha256(content).hexdigest())
        receipt.update(assess_manifest(json.loads(content)))
        receipt["assessor_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    except (OSError, KeyError, IndexError, TypeError, ValueError) as error:
        receipt["error"] = type(error).__name__ + ": " + str(error)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as output:
        json.dump(receipt, output, indent=2, sort_keys=True, allow_nan=False); output.write("\n")
    print(json.dumps({key: receipt[key] for key in ("assessment_complete", "all_trials_passed")}))
    return 0 if receipt["assessment_complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
