#!/usr/bin/env python3
"""Reassess every predeclared diagnostic cell without rerunning benchmarks.

Only successful complete executions are eligible for numeric pairs; metrics
also require their exact full companion. Failure snapshots and sampled tails
remain separate from successful cumulative totals. This is a bounded synthetic
experiment and makes no capacity, long-run retention or replacement claim.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import copy
import json
from pathlib import Path
import statistics
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import qualify_hyperfeed as gate
from run_controlled_campaign import make_plan


def read(path):
    return json.loads(Path(path).read_text())


def summary(values):
    values = [v for v in values if isinstance(v, (int, float)) and not isinstance(v, bool)]
    return {"n": len(values), "median": statistics.median(values),
            "min": min(values), "max": max(values)} if values else {"n": 0}


def selected(counts, prefix="conflict_origin:"):
    return {k: v for k, v in counts.items() if k.startswith(prefix)}


def summed_maps(maps):
    total = Counter()
    for value in maps:
        total.update(value)
    return dict(sorted(total.items()))


def snapshot_errors(value):
    errors = []
    if not isinstance(value, dict):
        return ["snapshot is not an object"]
    for field in ("metrics", "retry_causes", "diagnostics"):
        counts = value.get(field)
        if not isinstance(counts, dict) or any(not isinstance(k, str) or type(v) is not int or not 0 <= v < 2**64 for k, v in counts.items()):
            errors.append(f"malformed {field} map")
    return errors


def trace_summary(trace):
    samples = trace["samples"]
    return {"failed_attempts": trace["failed_attempts"], "retained_attempts": len(samples),
            "dropped_attempts": trace["dropped_attempts"],
            "primary_origins": summed_maps(selected(s["diagnostics_delta"]) for s in samples),
            "cleanup_origins": summed_maps(selected(s["cleanup_diagnostics_delta"]) for s in samples),
            "terminal_samples": [s for s in samples if s["error_kind"] != "conflict" or not s["cleanup_ok"] or s["attempt_index"] == 128],
            "scope": "Bounded last-attempt sample; overlaps cumulative maps and must not be added to them."}


def metric_values(run, assessment):
    classes = run.get("workload_classes", {})
    fg = classes.get("foreground", {})
    result = {"foreground_p99_ms": fg.get("p99_us_including_retries", 0) / 1000,
              "foreground_service_p99_ms": fg.get("service_latency_p99_us_including_retries", 0) / 1000,
              "foreground_queue_p99_ms": fg.get("arrival_queue_delay_p99_us", 0) / 1000,
              "foreground_retries": fg.get("retries"), "total_retries": run.get("retries"),
              "foreground_throughput_with_drain": assessment.get("foreground_throughput_with_drain"),
              "foreground_completed": fg.get("completed"),
              "elapsed_seconds_including_drain": run.get("elapsed_seconds_including_drain"),
              "completed_transactions": run.get("completed_transactions")}
    for name in ("projection", "housekeeping", "maintenance"):
        cls = classes.get(name, {})
        for field in ("offered", "completed", "positive_effect_jobs", "retries"):
            result[f"{name}_{field}"] = cls.get(field)
        result[f"{name}_p99_ms"] = cls.get("p99_us_including_retries", 0) / 1000
    return result


def failure_evidence(trial, enabled, errors):
    found = []
    directory = Path(trial["directory"])
    for path in sorted(directory.rglob("failure-progress.json")):
        progress = read(path)
        failure = progress.get("failure_evidence")
        local_errors = []
        detail = {"path": str(path), "sha256": gate.sha256(path),
                  "worker": progress.get("worker"), "error": progress.get("error"),
                  "completed_messages": progress.get("completed_messages"),
                  "completed_transactions": progress.get("completed_transactions"),
                  "completed_by_worker": progress.get("completed_by_worker"),
                  "offered_by_worker": progress.get("offered_by_worker"),
                  "pending_maintenance": progress.get("pending_maintenance"),
                  "counter_coverage": progress.get("counter_coverage"),
                  "failed_worker_cumulative": failure,
                  "completed_peer_snapshots": progress.get("completed_worker_metrics_snapshots"),
                  "completed_peer_traces": progress.get("worker_retry_diagnostics")}
        if progress.get("passed") is not False or progress.get("execution_completed") is not False:
            local_errors.append("failure progress declares successful execution")
        if failure is not None:
            local_errors += snapshot_errors(failure)
            if failure.get("version") != 1:
                local_errors.append("unknown failure evidence version")
            trace = failure.get("retry_diagnostics")
            trace_errors = gate.retry_trace_errors(trace, enabled)
            local_errors += trace_errors
            if not trace_errors:
                detail["failed_worker_tail"] = trace_summary(trace)
                # Reuse the exact freshness validator on a synthetic schema-only
                # sample, including failures with no retained attempts.
                fake = {"version": 1, "enabled": True, "sample_limit": 32,
                        "failed_attempts": 1, "dropped_attempts": 0, "samples": [{
                        "message_id": 0, "attempt_index": 0, "started_ns": 0,
                        "finished_ns": 0, "cleanup_finished_ns": 0, "error_kind": "fatal",
                        "error": "schema-only freshness check", "cleanup_ok": True,
                        "cleanup_error": None, "counter_regression": False,
                        "retry_causes_delta": {}, "diagnostics_delta": {},
                        "cleanup_retry_causes_delta": {}, "cleanup_diagnostics_delta": {},
                        "metrics_status": failure.get("metrics_status")}]}
                local_errors += gate.retry_trace_errors(fake, True)
            detail["failed_worker_cumulative_origins"] = selected(failure.get("diagnostics", {}))
        peers = progress.get("completed_worker_metrics_snapshots", [])
        traces = progress.get("worker_retry_diagnostics", [])
        if len(peers) != len(progress.get("completed_by_worker", [])) or len(traces) != len(peers):
            local_errors.append("failure snapshot worker coverage differs")
        for index, snapshot in enumerate(peers):
            if snapshot is not None:
                local_errors += [f"peer {index}: {e}" for e in snapshot_errors(snapshot)]
                if snapshot.get("scope") != "completed_worker_cumulative":
                    local_errors.append(f"peer {index}: wrong snapshot scope")
                local_errors += [f"peer {index}: {e}" for e in gate.retry_trace_errors(traces[index], enabled)]
        detail["validation_errors"] = local_errors
        errors.extend(f"{path}: {e}" for e in local_errors)
        found.append(detail)
    return found


def paired(cells, dimension):
    indexes = {(c["engine"], c["rate"], c["seed"], c["evidence"], c["expiry_index_policy"], c["retry_diagnostics"]): c for c in cells}
    pairs = []
    for cell in cells:
        if cell["engine"] == "postgres":
            continue
        if dimension == "runtime_diagnostics":
            if cell["retry_diagnostics"] != "off":
                continue
            other_key = (cell["engine"], cell["rate"], cell["seed"], cell["evidence"], cell["expiry_index_policy"], "on")
            fixed = {"expiry_index_policy": cell["expiry_index_policy"]}
        else:
            if cell["expiry_index_policy"] != "all-active":
                continue
            other_key = (cell["engine"], cell["rate"], cell["seed"], cell["evidence"], "housekeeping", cell["retry_diagnostics"])
            fixed = {"retry_diagnostics": cell["retry_diagnostics"]}
        other = indexes.get(other_key)
        pair = {"engine": cell["engine"], "rate": cell["rate"], "seed": cell["seed"],
                "evidence": cell["evidence"], **fixed, "baseline": cell["label"],
                "variant": other["label"] if other else None,
                "eligible": bool(other and cell["numeric_comparison_eligible"] and other["numeric_comparison_eligible"]),
                "baseline_reasons": cell["assessment"]["reasons"],
                "variant_reasons": other["assessment"]["reasons"] if other else ["missing cell"]}
        if pair["eligible"]:
            changes = {}
            for field, value in cell["metrics"].items():
                new = other["metrics"].get(field)
                if isinstance(value, (int, float)) and isinstance(new, (int, float)):
                    changes[field] = {"baseline": value, "variant": new, "delta": new-value,
                                      "ratio": new/value if value else None,
                                      "percent_change": 100*(new/value-1) if value else None}
            pair["changes"] = changes
        pairs.append(pair)
    groups = defaultdict(list)
    keys = ("engine", "rate", "evidence", "expiry_index_policy" if dimension == "runtime_diagnostics" else "retry_diagnostics")
    for pair in pairs:
        groups[tuple(pair[k] for k in keys)].append(pair)
    grouped = []
    for key, items in sorted(groups.items()):
        usable = [p for p in items if p["eligible"]]
        fields = sorted({field for p in usable for field in p["changes"]})
        grouped.append({**dict(zip(keys, key)), "planned_seeds": [p["seed"] for p in items],
                        "paired_seeds": [p["seed"] for p in usable], "missing_or_failed_pairs": len(items)-len(usable),
                        "all_three_seeds_eligible": len(usable) == 3,
                        "changes": {f: {unit: summary(p["changes"][f][unit] for p in usable)
                                        for unit in ("delta", "ratio", "percent_change")} for f in fields}})
    return {"direction": "diagnostics on / off" if dimension == "runtime_diagnostics" else "housekeeping / all-active",
            "scope": "Same engine, offered rate, seed and evidence mode. Summaries exclude failed/incomplete/unqualified companion cells and explicitly count those pairs. Full and metrics never pool.",
            "pairs": pairs, "groups": grouped}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", type=Path, default=Path(__file__).resolve().parent / "controlled-campaign")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    base = args.campaign.resolve(strict=True)
    execution_path = base / "execution.json"
    execution = read(execution_path)
    if not execution.get("completed") or not execution.get("passed") or not execution.get("source_stable"):
        parser.error("campaign is still running or collection did not complete stably; preserve evidence and review explicitly")
    output = args.output or base.parent / "controlled-review.json"
    errors, cells, campaigns, artifacts = [], [], {}, {}
    steps = execution["steps"]
    native = next(step["engine"] for step in steps if step["engine"] != "postgres")
    plan = make_plan(native, any(step["engine"] == "postgres" for step in steps))
    if len(steps) != len(plan) or execution.get("unrun_steps"):
        errors.append("predeclared cells were omitted")
    for step, expected in zip(steps, plan):
        if any(step.get(k) != v for k, v in expected.items()):
            errors.append(f"{step.get('label')}: execution changed its predeclared plan")
        path = Path(step["campaign_path"])
        campaigns[step["index"]] = read(path)
        artifacts[str(path)] = gate.sha256(path)
        if artifacts[str(path)] != step["campaign_sha256"]:
            errors.append(f"{step['label']}: campaign hash changed")
    for step in steps:
        campaign = campaigns[step["index"]]
        if len(campaign.get("trials", [])) != 1:
            errors.append(f"{step['label']}: expected exactly one trial")
        trial = campaign["trials"][0]
        source = execution["source_before"]
        binary = execution["binary_before_sha256"]
        if not gate.compatible_companion(campaign, source, binary):
            errors.append(f"{step['label']}: source/binary campaign provenance differs")
        if (trial.get("source_before_sha256") != source["sha256"] or trial.get("source_after_sha256") != source["sha256"]
                or trial.get("binary_before_sha256") != binary or trial.get("binary_after_sha256") != binary):
            errors.append(f"{step['label']}: trial source/binary provenance differs")
        expected_config = {"engine": step["engine"], "arrival_rate": step["rate"], "seed": step["seed"],
                           "expiry_index_policy": step["expiry_index_policy"],
                           "retry_diagnostics": step["retry_diagnostics"] == "on", "evidence": step["evidence"]}
        if any(trial.get("config", {}).get(k) != v for k, v in expected_config.items()):
            errors.append(f"{step['label']}: trial configuration differs from planned cell")
        for name, payload in (("trial.json", trial), ("report.json", trial.get("report"))):
            path = Path(trial["directory"]) / name
            if not path.exists() or read(path) != payload:
                errors.append(f"{step['label']}: raw {name} differs from campaign")
            elif path.exists():
                artifacts[str(path)] = gate.sha256(path)
        companion_keys = set()
        companion_detail = None
        if step["evidence"] == "metrics":
            companion_step = steps[step["full_companion_step"]]
            companion = campaigns[step["full_companion_step"]]
            full = companion["trials"][0]
            linked = campaign.get("correctness_report", {})
            exact = (gate.compatible_companion(companion, source, binary)
                     and gate.key(full["config"]) == gate.key(trial["config"])
                     and linked.get("path") == companion_step["campaign_path"]
                     and linked.get("sha256") == companion_step["campaign_sha256"])
            full_assessment = gate.assess_trial(copy.deepcopy(full), campaign["policy"])
            if exact and full_assessment["history_verified"]:
                companion_keys.add(gate.key(full["config"]))
            if not exact:
                errors.append(f"{step['label']}: companion does not bind exact planned full cell")
            companion_detail = {"label": companion_step["label"], "exact_binding": exact,
                                "history_verified": full_assessment["history_verified"],
                                "reasons": full_assessment["reasons"]}
        reassessed = gate.assess_trial(copy.deepcopy(trial), campaign["policy"], companion_keys)
        if reassessed != trial["assessment"] or reassessed != step["assessment"]:
            errors.append(f"{step['label']}: independent reassessment differs")
        runs = trial.get("report", {}).get("runs", [])
        run = runs[0] if len(runs) == 1 else {}
        cell = {k: step[k] for k in ("index", "label", "engine", "rate", "seed", "expiry_index_policy", "retry_diagnostics", "evidence")}
        cell.update(assessment=reassessed, exact_full_companion=companion_detail,
                    exit_code=trial.get("exit_code"), timed_out=trial.get("timed_out"),
                    benchmark_error=run.get("error"),
                    numeric_comparison_eligible=bool(reassessed["execution_valid"] and reassessed["correctness_companion_verified"] and reassessed.get("continuous_timing_passed") and reassessed.get("useful_work_passed")),
                    metrics=metric_values(run, reassessed) if reassessed["execution_valid"] else None,
                    failure_evidence=failure_evidence(trial, step["retry_diagnostics"] == "on", errors))
        if reassessed["execution_valid"]:
            trace_errors = gate.experiment_report_errors(run, trial["config"])
            errors.extend(f"{step['label']}: {e}" for e in trace_errors)
            cell["successful_cumulative_origins"] = selected(run.get("operation_diagnostics", {}))
            cell["successful_cumulative_retry_causes"] = run.get("retry_causes", {})
            cell["successful_cumulative_query_diagnostics"] = {k: v for k, v in run.get("operation_diagnostics", {}).items() if not k.startswith("conflict_origin:")}
            cell["successful_worker_tails"] = [trace_summary(t) for t in run.get("worker_retry_diagnostics", [])]
        cells.append(cell)
    grouped = defaultdict(list)
    fields = ("engine", "rate", "expiry_index_policy", "retry_diagnostics", "evidence")
    for cell in cells:
        grouped[tuple(cell[k] for k in fields)].append(cell)
    groups = []
    for key, items in sorted(grouped.items()):
        success = [c for c in items if c["assessment"]["execution_valid"]]
        qualified = [c for c in success if c["numeric_comparison_eligible"]]
        metric_names = sorted({k for c in qualified for k in c["metrics"]})
        groups.append({**dict(zip(fields, key)), "planned": len(items), "execution_valid": len(success),
                       "execution_failed": len(items)-len(success), "numeric_comparison_eligible": len(qualified),
                       "failed_full_companions": [c["label"] for c in items if c["exact_full_companion"] and not c["exact_full_companion"]["history_verified"]],
                       "seeds": [{"seed": c["seed"], "label": c["label"], "execution_valid": c["assessment"]["execution_valid"],
                                  "numeric_comparison_eligible": c["numeric_comparison_eligible"], "metrics": c["metrics"]} for c in items],
                       "qualified_metric_summary": {f: summary(c["metrics"][f] for c in qualified) for f in metric_names},
                       "successful_cumulative_origins": summed_maps(c.get("successful_cumulative_origins", {}) for c in success),
                       "origin_scope": "Successful complete cells only. Failed-worker snapshots and tails remain separate at cell level."})
    result = {"passed": not errors, "errors": errors, "planned_cells": len(plan), "reviewed_cells": len(cells),
              "execution_json": str(execution_path), "execution_sha256": gate.sha256(execution_path),
              "review_script_sha256": gate.sha256(Path(__file__)), "qualifier_sha256": gate.sha256(ROOT / "scripts/qualify_hyperfeed.py"),
              "artifact_hashes": artifacts, "source_sha256": execution["source_before"]["sha256"],
              "binary_sha256": execution["binary_before_sha256"],
              "scope": "Revalidated source/binary binding, plan, raw/campaign equality, assessments, exact companions, trace and failure schemas. Does not independently replay serial histories or establish capacity/10x/retention/physical MMHF.",
              "materialization_counter_scope": "Adapter reads following index lookup; candidate MVCC reads internal to lookup are not counted as additional attempted materializations.",
              "numeric_comparison_eligibility": "Complete execution, correct history or exact full companion, continuous drain timing, and useful_work_passed are all required. Other complete-cell metrics remain visible but do not enter paired performance summaries.",
              "censoring_scope": "Failed runs have no numeric throughput/latency contribution. Their receipts and partial cumulative snapshots remain separately visible; neither successful-only ratios nor tiny three-seed tails establish sustainable performance.",
              "cells": cells, "groups": groups,
              "runtime_diagnostic_pairs": paired(cells, "runtime_diagnostics"),
              "expiry_policy_pairs": paired(cells, "expiry_policy")}
    gate.atomic_json(output, result)
    print(json.dumps({"passed": result["passed"], "reviewed_cells": len(cells),
                      "execution_valid": sum(c["assessment"]["execution_valid"] for c in cells),
                      "numeric_comparison_eligible": sum(c["numeric_comparison_eligible"] for c in cells),
                      "errors": errors, "output": str(output)}, indent=2))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
