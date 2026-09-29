#!/usr/bin/env python3
"""Short, paired HyperFeed screens; these never establish sustained capacity.

Capture/source bindings and process ownership belong to the launcher. This
module checks original qualification reports and complete resource receipts.
Its lane policies are separate from the unchanged sustained-capacity policy.
"""
from __future__ import annotations

from statistics import median

import assess_hyperfeed_capacity as capacity
import qualify_hyperfeed as gate


LANES = {
    "foreground": {"seconds": 30, "projection_interval_seconds": 300,
                   "housekeeping_interval_seconds": 300, "warmup_seconds": 5,
                   "minimum_completed_maintenance_jobs": 0,
                   "minimum_positive_maintenance_jobs": 0},
    # Observe several recurring CPU bursts without changing the foreground
    # contract or implying that this screen establishes sustained capacity.
    "burst": {"seconds": 120, "projection_interval_seconds": 300,
              "housekeeping_interval_seconds": 300, "warmup_seconds": 5,
              "minimum_completed_maintenance_jobs": 0,
              "minimum_positive_maintenance_jobs": 0},
    "maintenance": {"seconds": 40, "projection_interval_seconds": 5,
                    "housekeeping_interval_seconds": 5, "warmup_seconds": 5,
                    "minimum_completed_maintenance_jobs": 3,
                    "minimum_positive_maintenance_jobs": 2},
}
COMPARISON_POLICY = {
    "required_pairs": 2,
    # One local A/A control showed an apparent ~10% p99 improvement. This
    # practical selection margin is not a confidence bound or a noise model.
    "p99_improvement_fraction": .20,
    "p99_regression_fraction": .10,
    "p99_absolute_change_ms": 1.0,
    "throughput_change_fraction": .01,
    "relative_range_noise_max": .25,
}


def screen_policy(value):
    """Return a declared lane policy without changing capacity.DEFAULT_POLICY."""
    supplied = {"lane": value} if isinstance(value, str) else dict(value)
    lane = supplied.pop("lane")
    if lane not in LANES:
        raise ValueError("unknown screen lane")
    allowed = set(capacity.DEFAULT_POLICY) | set(LANES[lane]) | {"expected_config"}
    if set(supplied) - allowed:
        raise ValueError("unknown screen policy field")
    result = {**capacity.DEFAULT_POLICY, **LANES[lane], **supplied, "lane": lane}
    for name in ("seconds", "projection_interval_seconds", "housekeeping_interval_seconds"):
        capacity.integer(result[name], name, 1)
    for name in capacity.DEFAULT_POLICY:
        capacity.number(result[name], name)
    for name in ("warmup_seconds", "minimum_completed_maintenance_jobs", "minimum_positive_maintenance_jobs"):
        capacity.integer(result[name], name)
    if not result["warmup_seconds"] < result["seconds"]:
        raise ValueError("screen warmup must end before admission")
    for name in ("minimum_completed_fraction", "minimum_final_third_completed_fraction"):
        if not 0 < result[name] <= 1:
            raise ValueError("screen completion fraction must be in (0, 1]")
    if not isinstance(result.get("expected_config", {}), dict):
        raise ValueError("expected_config must be an object")
    if lane in {"foreground", "burst"} and any(result[name] < result["seconds"] for name in gate.CALIBRATED_FIELDS):
        raise ValueError("foreground screen must end before its first maintenance tick")
    if lane == "maintenance" and any((result["seconds"] - 1) // result[name] < 3 for name in gate.CALIBRATED_FIELDS):
        raise ValueError("maintenance screen requires at least three scheduled jobs per class")
    return result


def lane_policy(name):
    """Public preset constructor for launchers; returns a fresh policy object."""
    return screen_policy(name)


def assess_screen_trial(trial, envelope, lane_policy):
    """Assess a qualify trial and {result, readiness, samples} resource bundle.

    A valid_measurement is a completed, audited metrics run. Operational
    failures remain evidence, but do not invent a complete throughput/p99 pair.
    Optional trial.failure_progress may carry original partial accounting.
    """
    result = {"classification": "invalid_evidence", "valid_measurement": False,
              "screen_requirements_met": False, "screening_only": True,
              "sustained_capacity_established": False, "reasons": [], "metrics": {}}
    try:
        policy = screen_policy(lane_policy)
        config = trial["config"]
        result.update(lane=policy["lane"], config=config,
                      binary_sha256=trial.get("binary_before_sha256"),
                      source_sha256=trial.get("source_before_sha256"))
        expected = {"workload": "calibrated", "evidence": "metrics",
                    "maintenance_mode": "sweep", "rolling_cycle_messages": 0,
                    **{name: policy[name] for name in ("seconds", *gate.CALIBRATED_FIELDS)},
                    **policy.get("expected_config", {})}
        if any(type(gate.config_value(config, k)) is not type(v) or gate.config_value(config, k) != v
               for k, v in expected.items()):
            raise ValueError("trial configuration differs from declared screen lane")
        if config.get("engine") not in {"aerostore", "service-unix", "service-tcp"}:
            raise ValueError("screen requires a native engine configuration")
        if trial.get("source_stable") is not True:
            raise ValueError("source or binary changed during screen")
        resources = capacity.resource_assessment(envelope["result"], envelope["readiness"],
                                                 envelope["samples"], policy)
        result["resources"] = resources
        if not resources["passed"]:
            result.update(classification="censored_resources", reasons=resources["reasons"])
            return result
        failure = trial.get("failure_progress")
        category, reason, error = capacity.error_classification(trial, failure)
        result["error_text"] = error
        partial = (failure or {}).get("capacity_accounting")
        if partial:
            observed = capacity.accounting_assessment(partial, config, {}, policy, partial=True)
            result["partial_accounting"] = observed
            if category == "censored_timeout" and any(x["unfinished_past_deadline"] for x in observed["maintenance"].values()):
                category, reason = "operational_failure", "maintenance_starvation_observed_before_timeout"
        if category:
            if reason == "worker_progress_stall" and (not partial or partial["foreground"].get("offered_observed", 0) <= 0):
                category, reason = "censored_timeout", "stall lacks observed positive foreground arrivals"
            result.update(classification=category, reasons=[reason])
            return result
        check = gate.assess_trial(trial, {"slo_ms": policy["foreground_p99_ms"],
            "minimum_drain_fraction": .99, "max_noop_fraction": 0, "outcome_tolerance": 0})
        result["structural_assessment"] = {name: check.get(name) for name in (
            "execution_valid", "continuous_timing_passed", "useful_work_passed",
            "foreground_ordering_passed", "history_verified", "global_maintenance_sweep_complete")}
        result["arena_backing"] = check.get("arena_backing")
        required = ["execution_valid", "continuous_timing_passed", "useful_work_passed", "foreground_ordering_passed"]
        if policy["lane"] == "maintenance":
            required.append("global_maintenance_sweep_complete")
        if not all(check.get(name) is True for name in required):
            raise ValueError("screen execution/ordering/useful-work audit failed: " + "; ".join(check["reasons"]))
        run = trial["report"]["runs"][0]
        storage = capacity.storage_assessment(run, config["engine"])
        result["storage"] = storage
        if not storage["passed"]:
            result.update(classification="operational_failure", reasons=storage["reasons"])
            return result
        measured = capacity.accounting_assessment(run["capacity_accounting"], config, run, policy)
        foreground = measured["foreground_offered"]
        result["metrics"] = {**measured,
            "completed_messages_per_second": measured["foreground_received_in_admission"] / config["seconds"],
            "offered_messages_per_second": config["arrival_rate"],
            "foreground_retries_per_message": run["workload_classes"]["foreground"]["retries"] / foreground,
            "all_transaction_retries_per_message": run["retries"] / foreground,
            "resource_peak_bytes": resources["peak_bytes"]}
        passed = not measured["reasons"]
        result.update(classification="screen_passed" if passed else "completed_policy_failure",
                      valid_measurement=True, screen_requirements_met=passed, reasons=measured["reasons"],
                      maintenance_scope="not_observed_first_tick_after_screen" if policy["lane"] in {"foreground", "burst"}
                      else "accelerated_stress_not_representative_cadence")
    except (KeyError, ValueError, TypeError, IndexError, ZeroDivisionError, OverflowError) as error:
        result.update(classification="invalid_evidence", valid_measurement=False,
                      screen_requirements_met=False, reasons=[str(error)])
    return result


def compare_screens(rows, policy=None):
    """Compare {variant, seed, lane, assessment} rows with exact paired coverage.

    Optional policy seeds/lanes declare expected coverage. Other thresholds are
    COMPARISON_POLICY keys. Thresholds select experiments, not statistical claims.
    """
    output = {"classification": "inconclusive", "screening_only": True,
              "sustained_capacity_established": False, "capacity_gain_established": False,
              "needs_sustained_qualification": True, "reasons": [], "pairs": []}
    try:
        supplied = dict(policy or {})
        if set(supplied) - (set(COMPARISON_POLICY) | {"seeds", "lanes", "identical_binary"}):
            raise ValueError("unknown comparison policy field")
        settings = {**COMPARISON_POLICY, **supplied}
        capacity.integer(settings["required_pairs"], "required pairs", 2)
        for name in COMPARISON_POLICY:
            if name != "required_pairs":
                capacity.number(settings[name], name)
        if not rows:
            raise ValueError("no paired screens")
        control_hashes = {row.get("assessment", {}).get("binary_sha256") for row in rows}
        measured_control = len(control_hashes) == 1 and None not in control_hashes
        output["same_binary_control"] = measured_control
        if measured_control or settings.get("identical_binary") is True:
            output["decision"] = "control_only"
        grouped = {}
        for row in rows:
            if row["variant"] not in {"baseline", "candidate"}:
                raise ValueError("unknown screen variant")
            assessment = row["assessment"]
            if row["lane"] != assessment.get("lane") or row["seed"] != assessment["config"]["seed"]:
                raise ValueError("screen label differs from measured lane/seed")
            key = (row["lane"], row["seed"])
            group = grouped.setdefault(key, {})
            if row["variant"] in group:
                raise ValueError("duplicate paired screen")
            group[row["variant"]] = assessment
        lanes = settings.get("lanes", sorted({key[0] for key in grouped}))
        seeds = settings.get("seeds", sorted({key[1] for key in grouped}))
        if len(set(lanes)) != len(lanes) or len(set(seeds)) != len(seeds) or len(seeds) < settings["required_pairs"]:
            raise ValueError("insufficient or duplicated declared pair coverage")
        if set(grouped) != {(lane, seed) for lane in lanes for seed in seeds}:
            raise ValueError("missing or unexpected screen lane/seed")
        identities = {"baseline": set(), "candidate": set()}
        for (lane, seed), group in sorted(grouped.items()):
            if set(group) != {"baseline", "candidate"}:
                raise ValueError("missing paired variant")
            a, b = group["baseline"], group["candidate"]
            configs = [{name: gate.config_value(row["config"], name) for name in gate.match_fields(row["config"]) + ("evidence",)} for row in (a, b)]
            if configs[0] != configs[1]:
                raise ValueError("paired configurations differ")
            if gate.arena_storage_identity(a.get("arena_backing")) != gate.arena_storage_identity(b.get("arena_backing")):
                raise ValueError("paired arena filesystems or storage lifetimes differ")
            for variant, assessment in group.items():
                binary = assessment.get("binary_sha256")
                if not isinstance(binary, str) or len(binary) != 64:
                    raise ValueError("missing measured binary identity")
                identities[variant].add(binary)
            if b["classification"] in {"correctness_failure", "operational_failure"} and a["valid_measurement"]:
                output.update(classification="inconclusive" if measured_control else "regression",
                              reasons=["candidate correctness or operational failure"],
                              failure_pair={"lane": lane, "seed": seed, "reasons": b["reasons"]})
                output.setdefault("decision", output["classification"])
                return output
            if not all(row["valid_measurement"] for row in (a, b)):
                raise ValueError("paired measurement incomplete, censored, or invalid")
            ma, mb = a["metrics"], b["metrics"]
            ap, bp = ma["foreground_p99_ms"], mb["foreground_p99_ms"]
            at, bt = ma["completed_messages_per_second"], mb["completed_messages_per_second"]
            if min(ap, bp, at, bt) <= 0:
                raise ValueError("nonpositive comparison metric")
            output["pairs"].append({"lane": lane, "seed": seed,
                "baseline_requirements_met": a["screen_requirements_met"],
                "candidate_requirements_met": b["screen_requirements_met"],
                "p99_ratio": bp / ap, "p99_change_ms": bp - ap,
                "completed_throughput_ratio": bt / at,
                "baseline_p99_ms": ap, "candidate_p99_ms": bp,
                "baseline_completed_messages_per_second": at, "candidate_completed_messages_per_second": bt})
        if any(len(value) != 1 for value in identities.values()):
            raise ValueError("a variant changed binaries between screen pairs")
        if "identical_binary" in settings and type(settings["identical_binary"]) is not bool:
            raise ValueError("identical_binary must be a boolean")
        same_binary = identities["baseline"] == identities["candidate"]
        if settings.get("identical_binary") is True and not same_binary:
            raise ValueError("declared identical-binary control has different measured binaries")
        output["same_binary_control"] = same_binary
        if same_binary:
            output["decision"] = "control_only"
        lane_results = []
        for lane in lanes:
            pairs = [row for row in output["pairs"] if row["lane"] == lane]
            noisy = any((max(values) - min(values)) / median(values) > settings["relative_range_noise_max"]
                        for values in ([r[prefix + "_p99_ms"] for r in pairs] for prefix in ("baseline", "candidate")))
            slower = all((r["p99_ratio"] >= 1 + settings["p99_regression_fraction"]
                          and r["p99_change_ms"] >= settings["p99_absolute_change_ms"])
                         or r["completed_throughput_ratio"] <= 1 - settings["throughput_change_fraction"] for r in pairs)
            faster = all((r["p99_ratio"] <= 1 - settings["p99_improvement_fraction"]
                          and r["p99_change_ms"] <= -settings["p99_absolute_change_ms"])
                         or r["completed_throughput_ratio"] >= 1 + settings["throughput_change_fraction"] for r in pairs)
            loses_requirements = all(r["baseline_requirements_met"] and not r["candidate_requirements_met"] for r in pairs)
            wins_requirements = all(not r["baseline_requirements_met"] and r["candidate_requirements_met"] for r in pairs)
            status = "inconclusive" if noisy else "regression" if slower or loses_requirements else "promising" if (faster or wins_requirements) and all(r["candidate_requirements_met"] for r in pairs) else "neutral"
            if same_binary and status in {"promising", "regression"}:
                status = "inconclusive"
            lane_results.append({"lane": lane, "classification": status, "noisy": noisy,
                "median_p99_ratio": median(r["p99_ratio"] for r in pairs),
                "median_completed_throughput_ratio": median(r["completed_throughput_ratio"] for r in pairs)})
        output["lanes"] = lane_results
        statuses = {r["classification"] for r in lane_results}
        output["classification"] = "regression" if "regression" in statuses else "inconclusive" if "inconclusive" in statuses else "promising" if "promising" in statuses else "neutral"
        output.setdefault("decision", output["classification"])
        output["interpretation"] = "Short-screen selection evidence only. Equal completed throughput with lower p99 is latency headroom; offered load caps throughput. Qualify sustained capacity before claiming a capacity gain."
    except (KeyError, ValueError, TypeError, IndexError, ZeroDivisionError, OverflowError) as error:
        output.update(classification="inconclusive", reasons=[str(error)])
    output.setdefault("decision", output["classification"])
    return output
