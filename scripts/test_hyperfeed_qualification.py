#!/usr/bin/env python3
"""Adversarial gate tests; no database or benchmark is started."""
import copy
from collections import Counter
import contextlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import qualify_hyperfeed as gate


POLICY = {"slo_ms": 100, "max_noop_fraction": .25,
          "minimum_drain_fraction": .95, "outcome_tolerance": 0}
SEEDS = [11, 22, 33]


def trial(engine="aerostore", rate=32, workers=1, seed=11, evidence="full", p99_ms=1):
    config = {"engine": engine, "workload": "lifecycle", "evidence": evidence,
              "families": 4, "hot_percent": 80, "seed": seed, "arrival_rate": rate,
              "seconds": 5, "workers": workers, "pg_write_mode": "buffered", "rpc_delay_us": 0,
              "global_time_predicates": False, "shm_mib": 256, "max_backlog": 1000,
              "max_messages": 100000, "message_interval_us": 0}
    offered = rate * 5
    counts = gate.lifecycle_kind_counts(offered)
    outcomes = {name: 0 for name in gate.EFFECT_FIELDS + ("missing_family", "allocation_deferred", "ignored_stale")}
    outcomes.update(created_views=offered, updated_views=offered, outputs=offered,
                    expired_records=offered, expired_families=offered // 16)
    per_kind = {name: {"completed": count, "outcomes": {key: 0 for key in outcomes}} for name, count in counts.items()}
    per_kind["plan"]["outcomes"] = outcomes
    run = {"engine": engine, "scenario": "sustained-mixed", "passed": True, "execution_completed": True,
           "arrival_mode": "independent_fixed_corpus", "offered_messages": offered, "completed_messages": offered,
           "completed_by_worker": [(offered - 1 - worker) // workers + 1 for worker in range(workers)],
           "admission_seconds": 5, "offered_rate_per_second": rate, "requested_duration_reached": True,
           "worker_message_cap_reached": False, "elapsed_seconds": 5.001,
           "admission_started_ns": 1_000_000_000, "workload_completed_ns": 6_001_000_000,
           "workers_stopped_ns": 6_003_000_000, "drain_confirmed_ns": 6_005_000_000,
           "elapsed_seconds_including_drain": 5.005, "timing_scope": "continuous_client_monotonic",
           "worker_stops": [{"stop_reason": "arrival_corpus_drained"} for _ in range(workers)],
           "invariants": {"passed": True}, "store_metrics": {"commits": offered},
           "history_checked": evidence == "full", "correctness_history_verified": evidence == "full",
           "oracle_status": "Valid" if evidence == "full" else "NotCheckedMetricsOnly",
           "message_latency_p99_us_including_retries": p99_ms * 1000,
           "completed_messages_per_second_including_drain": offered / 5.005,
           "message_kinds": counts, "per_kind": per_kind}
    return {"config": config, "exit_code": 0, "timed_out": False, "source_stable": True,
            "report": {"completed": True, "passed": True, "config": {**config, "mode": "sustained"}, "runs": [run]}}


def fleet_trial():
    evidence = trial(rate=65, seed=20260925)
    config = evidence["config"]
    config.update(workload="fleet", families=16, hot_percent=0)
    evidence["report"]["config"].update(config)
    run = evidence["report"]["runs"][0]
    outcomes = gate.collect_outcomes(run)
    counts = gate.expected_kind_counts(config, run["completed_messages"])
    run["message_kinds"] = counts
    run["per_kind"] = {name: {"completed": count, "outcomes": {field: 0 for field in outcomes}} for name, count in counts.items()}
    run["per_kind"]["plan"]["outcomes"] = outcomes
    run["per_kind"]["global_projection"]["outcomes"]["claimed_events"] = 4
    run["per_kind"]["global_reschedule"]["outcomes"]["rescheduled_events"] = 4
    run["per_kind"]["global_cancel"]["outcomes"]["cancelled_events"] = 4
    run["per_kind"]["global_housekeeping"]["outcomes"]["expired_records"] = 12
    run["initial_fleet"] = {"live_families": 11}
    run["final_fleet"] = {"live_families": 12}
    return evidence


def calibrated_trial(seconds=1201, rate=1, workers=4, families=16,
                     projection=300, housekeeping=600, evidence="full", p99_ms=1):
    item = trial(rate=rate, workers=workers, evidence=evidence, p99_ms=p99_ms)
    config = item["config"]
    config.update(workload="calibrated", seconds=seconds, families=families, hot_percent=0,
                  projection_interval_seconds=projection, housekeeping_interval_seconds=housekeeping)
    item["report"]["config"].update(config)
    active = families - max(1, families // 4)
    foreground = rate * seconds
    # Enumerate the input lanes in fixtures, independently of the driver's
    # closed-form corpus arithmetic. Timer endpoint cases have golden tests.
    workers_completed = [0] * (workers + 2)
    kinds = Counter()
    for sequence in range(foreground):
        identity, ordinal = sequence % active, sequence // active
        workers_completed[identity % workers] += 1
        kinds["plan" if ordinal % 16 == 0 else "position"] += 1
    projections = len(range(projection, seconds, projection))
    cleanups = len(range(housekeeping, seconds, housekeeping))
    for name, worker, count in (("global_projection", workers, projections),
                                ("global_housekeeping", workers + 1, cleanups)):
        workers_completed[worker] = count
        if count:
            kinds[name] = count
    total = foreground + projections + cleanups
    per_kind = {}
    for name, count in kinds.items():
        outcomes = {field: 0 for field in gate.EFFECT_FIELDS + ("missing_family", "allocation_deferred", "ignored_stale")}
        if name in ("plan", "position"):
            outcomes.update(updated_views=count, outputs=count)
        elif name == "global_projection":
            outcomes.update(claimed_events=4 * count, rescheduled_events=4 * count, outputs=4 * count)
        else:
            outcomes["expired_records"] = 32 * count
        per_kind[name] = {"completed": count, "outcomes": outcomes}
    run = item["report"]["runs"][0]
    run.update(arrival_mode="calibrated_fixed_timeline", offered_rate_scope="foreground_only",
               offered_messages=total, completed_messages=total, completed_by_worker=workers_completed,
               admission_seconds=seconds, elapsed_seconds=seconds + .001,
               workload_completed_ns=1_000_000_000 + seconds * 1_000_000_000 + 1_000_000,
               workers_stopped_ns=1_000_000_000 + seconds * 1_000_000_000 + 3_000_000,
               drain_confirmed_ns=1_000_000_000 + seconds * 1_000_000_000 + 5_000_000,
               elapsed_seconds_including_drain=seconds + .005,
               completed_messages_per_second_including_drain=total / (seconds + .005),
               worker_stops=[{"stop_reason": "arrival_corpus_drained"} for _ in workers_completed],
               store_metrics={"commits": total}, message_kinds=dict(kinds), per_kind=per_kind,
               total_process_workers=workers + 2, retries=0,
               initial_fleet={"live_families": families}, final_fleet={"live_families": families},
               per_flight_order={"checked": True, "passed": True, "foreground_completions": foreground,
                                 "identities_observed": min(foreground, active)},
               calibrated_schedule={"foreground_workers": workers, "maintenance_workers": 2,
                                    "active_families": active, "quiet_families": families - active,
                                    "projection_interval_seconds": projection, "housekeeping_interval_seconds": housekeeping,
                                    "first_timer_tick": "after_one_interval", "timer_admission": "strictly_before_end",
                                    "clock": "wall_clock", "cadence": "accelerated" if min(projection, housekeeping) < 300 else "representative_interval_config" if max(projection, housekeeping) <= 600 else "custom_outside_calibration",
                                    "per_flight_ordering": "stable_foreground_worker_fifo",
                                    "projection_batch_limit": 4, "housekeeping_batch_limit": 32,
                                    "maintenance_scope": "bounded_batch_not_full_sweep",
                                    "population_turnover_tested": False, "global_maintenance_sweep_complete": False})
    run["workload_classes"] = {}
    for name, count in (("foreground", foreground), ("projection", projections),
                        ("housekeeping", cleanups), ("maintenance", projections + cleanups)):
        run["workload_classes"][name] = {
            "offered": count, "completed": count, "retries": 0, "positive_effect_jobs": count,
            "p99_us_including_retries": p99_ms * 1000 if count else None,
            "service_latency_p99_us_including_retries": 500 if count else None,
            "arrival_queue_delay_p99_us": 50 if count else None,
        }
    run["worker_activity"] = [
        {"worker": index, "role": "foreground" if index < workers else ("projection" if index == workers else "housekeeping"),
         "offered": count, "completed": count, "retries": 0, "busy_ns": count * 1_000_000,
         "utilization": count * 1_000_000 / ((seconds + .005) * 1e9)}
        for index, count in enumerate(workers_completed)
    ]
    return item


def routed_trial(dispatch="signature-affinity", ttl=1000, pattern="mixed", **options):
    item = calibrated_trial(**options)
    config = item["config"]
    config.update(dispatch=dispatch, affinity_ttl_ms=ttl, signature_pattern=pattern)
    item["report"]["config"].update(config)
    run = item["report"]["runs"][0]
    audit = copy.deepcopy(gate.calibrated_dispatch(config))
    run["dispatch_audit"] = {**audit, "checked": True, "passed": True}
    counts = audit["worker_counts"] + run["completed_by_worker"][-2:]
    run["completed_by_worker"] = counts
    for statistics, count in zip(run["worker_activity"], counts):
        statistics.update(offered=count, completed=count, busy_ns=count * 1000000,
                          utilization=count * 1000000 / (run["elapsed_seconds_including_drain"] * 1e9))
    if dispatch == "signature-affinity":
        run["calibrated_schedule"]["per_flight_ordering"] = "signature_affinity_worker_fifo"
        run["per_flight_order"].update(required=False, overlapping_messages=0, out_of_order_completions=0)
    return item


def sweep_trial(evidence="full", seconds=5, projection_batch_size=4, housekeeping_batch_size=32,
                max_maintenance_batches=4096, empty=False):
    item = calibrated_trial(seconds=seconds, rate=6, workers=2, families=4,
                            projection=1, housekeeping=2, evidence=evidence)
    config = item["config"]
    config.update(maintenance_mode="sweep", projection_batch_size=projection_batch_size,
                  housekeeping_batch_size=housekeeping_batch_size, max_maintenance_batches=max_maintenance_batches)
    item["report"]["config"].update(config)
    run = item["report"]["runs"][0]
    run["calibrated_schedule"].update(maintenance_mode="sweep", max_maintenance_batches=max_maintenance_batches,
                                    projection_batch_limit=projection_batch_size, housekeeping_batch_limit=housekeeping_batch_size,
                                    maintenance_scope="complete_sweep_batched_transactions",
                                    global_maintenance_sweep_complete=seconds > 1)
    jobs = []
    for name, worker, interval, limit, bit in (("projection", 2, 1, projection_batch_size, 0),
                                             ("housekeeping", 3, 2, housekeeping_batch_size, 1)):
        kind = "global_" + name
        selected = []
        for ordinal, tick in enumerate(range(interval, seconds, interval)):
            rows = 0 if empty else limit + 1
            batches = 1 if empty else 3
            outcomes = {field: 0 for field in gate.OUTCOME_FIELDS}
            if name == "projection":
                outcomes.update(claimed_events=rows, rescheduled_events=rows, outputs=rows)
            else:
                outcomes["expired_records"] = rows
            scheduled = run["admission_started_ns"] + tick * 1000000000
            first_id = 8000000000 + (ordinal * 2 + bit) * 4096
            selected.append(dict(worker=worker, job_id=4000000000 + ordinal * 2 + bit,
                                 job_ordinal=ordinal, **{"class": name}, scheduled_ns=scheduled,
                                 started_ns=scheduled + 100000, finished_ns=scheduled + 100000 + batches * 1000000,
                                 received_ns=scheduled + 200000 + batches * 1000000,
                                 batches=batches, nonempty_batches=batches - 1, terminal_batches=1,
                                 processed_rows=rows, retries=int(name == "projection" and ordinal == 0), terminal_empty=True,
                                 first_transaction_id=first_id, terminal_transaction_id=first_id + batches - 1,
                                 outcomes=outcomes))
        if selected:
            run["per_kind"][kind]["outcomes"] = {field: sum(job["outcomes"][field] for job in selected) for field in gate.OUTCOME_FIELDS}
        run["worker_activity"][worker].update(retries=sum(job["retries"] for job in selected),
                                              busy_ns=sum(job["finished_ns"] - job["started_ns"] for job in selected))
        run["worker_activity"][worker]["utilization"] = run["worker_activity"][worker]["busy_ns"] / (run["elapsed_seconds_including_drain"] * 1e9)
        jobs.extend(selected)
    # Fixture sizes are below 100, so nearest-rank p99 is the largest duration.
    for name in ("projection", "housekeeping", "maintenance"):
        selected = [job for job in jobs if name == "maintenance" or job["class"] == name]
        statistics = run["workload_classes"][name]
        statistics.update(retries=sum(job["retries"] for job in selected),
                          positive_effect_jobs=sum(job["processed_rows"] > 0 for job in selected))
        for field, end, start in (("p99_us_including_retries", "received_ns", "scheduled_ns"),
                                  ("service_latency_p99_us_including_retries", "finished_ns", "started_ns"),
                                  ("arrival_queue_delay_p99_us", "started_ns", "scheduled_ns")):
            statistics[field] = max((job[end] - job[start] for job in selected), default=0) / 1000 if selected else None
    run["maintenance_jobs"] = jobs
    run["maintenance_job_audit"] = dict(checked=True, passed=True, completed_jobs=len(jobs),
        committed_batches=sum(job["batches"] for job in jobs), nonempty_batches=sum(job["nonempty_batches"] for job in jobs),
        terminal_batches=len(jobs), processed_rows=sum(job["processed_rows"] for job in jobs),
        scope="complete_sweep_batched_transactions")
    run["transaction_kinds"] = dict(run["message_kinds"])
    for name in ("projection", "housekeeping"):
        selected = [job for job in jobs if job["class"] == name]
        if selected:
            run["transaction_kinds"]["global_" + name] = sum(job["batches"] for job in selected)
    for kind, count in run["transaction_kinds"].items():
        run["per_kind"][kind]["transactions"] = count
    run["completed_transactions"] = sum(run["transaction_kinds"].values())
    run["store_metrics"]["commits"] = run["completed_transactions"]
    run["retries"] = sum(job["retries"] for job in jobs)
    return item


def rolling_trial(evidence="full", seconds=40, cycle=16, retention=1):
    item = sweep_trial(evidence=evidence, seconds=seconds)
    config = item["config"]
    config.update(rolling_cycle_messages=cycle, rolling_retention_seconds=retention)
    item["report"]["config"].update(config)
    run = item["report"]["runs"][0]
    # Explicit phase sequence provides an independent oracle for the gate's
    # arithmetic over complete and partial identity rounds.
    sequence = (["creation", "position", "source_growth", "position", "source_growth", "position"]
                + ["position"] * (cycle - 10) + ["arrival"] * 3 + ["retirement"])
    counts, positives, outcomes = Counter(), Counter(), {}
    created, retired = {}, 0
    active = config["families"] - max(1, config["families"] // 4)
    for q in range(config["arrival_rate"] * seconds):
        identity, ordinal = q % active, q // active
        generation, step = divmod(ordinal, cycle)
        phase = sequence[step]
        counts[phase] += 1
        positives.setdefault(phase, 0)
        row = outcomes.setdefault(phase, {name: 0 for name in gate.OUTCOME_FIELDS})
        if phase != "retirement":
            positives[phase] += 1
            row["updated_views"] += 1
            row["outputs"] += 1
        if phase == "creation":
            row["created_views"] += 1
            created.setdefault(identity * 2 + generation % 2, set()).add(generation)
        elif phase == "source_growth":
            row["created_views"] += 2 if step == 2 else 4
        elif phase == "retirement" and generation > 0:
            positives[phase] += 1
            row["expired_families"] += 1
            row["expired_records"] += 7
            retired += 1
    kinds = {"creation": "plan", "source_growth": "plan", "position": "position",
             "arrival": "arrival", "retirement": "expire_family"}
    run["per_kind"] = {name: row for name, row in run["per_kind"].items() if name.startswith("global_")}
    for phase, count in counts.items():
        row = run["per_kind"].setdefault(kinds[phase], {"completed": 0, "transactions": 0,
                                                       "outcomes": {name: 0 for name in gate.OUTCOME_FIELDS}})
        row["completed"] += count
        row["transactions"] += count
        for name, value in outcomes[phase].items():
            row["outcomes"][name] += value
    run["message_kinds"] = {name: row["completed"] for name, row in run["per_kind"].items()}
    run["transaction_kinds"] = {name: row["transactions"] for name, row in run["per_kind"].items()}
    run["rolling_lifecycle"] = {
        "enabled": True, "checked": True, "passed": True,
        "phase_counts": dict(counts), "phase_positive_effects": dict(positives), "phase_outcomes": outcomes,
        "created_generations": sum(len(values) for values in created.values()), "retired_families": retired,
        "reused_family_generations": sum(len(values) - 1 for values in created.values()),
        "physical_families_reused": sum(len(values) > 1 for values in created.values()),
    }
    turnover = bool(created and retired)
    run["population_turnover_tested"] = turnover
    run["calibrated_schedule"].update(rolling_cycle_messages=cycle, rolling_retention_seconds=retention,
        housekeeping_seed_cohorts=0, population_turnover_tested=turnover,
        rolling_policy={"enabled": True, "cycle_messages": cycle, "record_retention_seconds": retention,
            "policy": "rate_dependent_per_identity_lifecycle_v1", "event_clock": "offered_wall_time_nanoseconds",
            "maintenance_clock": "independent_wall_time_timers", "initial_population": "empty_when_enabled",
            "creation": "newer_generation_only_in_vacant_alternating_pool",
            "position_sources": "three_contiguous_blocks_rotated_by_generation",
            "message_id_stride": 1025, "message_id": "1000000+per_identity_ordinal*1025+logical_identity",
            "terminal_retention_seconds": retention + config["housekeeping_interval_seconds"],
            "retirement_generation": "planned_previous_generation_not_actual_row_evidence", "capacity_qualified": False})
    run["initial_fleet"]["live_families"] = 0
    run["final_fleet"]["live_families"] = sum(len(values) for values in created.values()) - retired
    run["workload_classes"]["foreground"]["positive_effect_jobs"] = sum(positives.values())
    return item


class QualificationTests(unittest.TestCase):
    def test_invalid_maintenance_bounds_and_noncalibrated_overrides_fail_before_execution(self):
        for flags in (["--projection-batch-size", "0"], ["--projection-batch-size", "17"],
                      ["--housekeeping-batch-size", "0"], ["--housekeeping-batch-size", "65"],
                      ["--max-maintenance-batches", "0"], ["--max-maintenance-batches", "4097"],
                      ["--workload", "fleet", "--maintenance-mode", "sweep"],
                      ["--workload", "lifecycle", "--projection-batch-size", "8"]):
            with self.subTest(flags=flags), tempfile.TemporaryDirectory() as directory:
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                    gate.main(["--binary", "/nonexistent/benchmark", "--output", directory,
                               "--workload", "calibrated", "--families", "16", "--hot-percent", "0",
                               "--slo-ms", "100", *flags])
                self.assertEqual(error.exception.code, 2)
                self.assertEqual(json.loads((Path(directory) / "campaign.json").read_text())["stage"], "configuration")

    def test_configurable_batch_control_retains_one_transaction_per_job(self):
        item = calibrated_trial()
        item["config"].update(maintenance_mode="batch", projection_batch_size=8, housekeeping_batch_size=64, max_maintenance_batches=1)
        item["report"]["config"].update(item["config"])
        run = item["report"]["runs"][0]
        run["calibrated_schedule"].update(maintenance_mode="batch", projection_batch_limit=8,
                                           housekeeping_batch_limit=64, max_maintenance_batches=1)
        run["completed_transactions"] = run["completed_messages"]
        run["transaction_kinds"] = dict(run["message_kinds"])
        for statistics in run["per_kind"].values():
            statistics["transactions"] = statistics["completed"]
        self.assertTrue(gate.assess_trial(item, POLICY)["execution_valid"])
        run["completed_transactions"] += 1
        self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])

    def test_complete_sweeps_count_jobs_separately_from_committed_transactions(self):
        item = sweep_trial()
        verdict = gate.assess_trial(item, POLICY)
        self.assertTrue(verdict["execution_valid"], verdict)
        self.assertTrue(verdict["history_verified"])
        self.assertTrue(verdict["global_maintenance_sweep_complete"])
        self.assertFalse(verdict["qualified_capacity_trial"])
        self.assertFalse(verdict["performance_passed"])
        run = item["report"]["runs"][0]
        self.assertEqual(run["completed_messages"], 36)
        self.assertEqual(run["completed_transactions"], 48)
        self.assertEqual(run["workload_classes"]["maintenance"]["completed"], 6)
        self.assertFalse(any("bounded batches" in reason for reason in verdict["qualification_limitations"]))

    def test_sweep_rejects_missing_terminal_forged_commits_job_inflation_and_caps(self):
        mutations = [lambda run: run["maintenance_jobs"].pop(),
                     lambda run: run["maintenance_jobs"].append(copy.deepcopy(run["maintenance_jobs"][0])),
                     lambda run: run["maintenance_jobs"][0].update(terminal_empty=False),
                     lambda run: run["maintenance_jobs"][0].update(terminal_batches=0),
                     lambda run: run["maintenance_jobs"][0].update(batches=2),
                     lambda run: run["maintenance_jobs"][0].update(batches=4097, nonempty_batches=4096),
                     lambda run: run["maintenance_jobs"][0].update(processed_rows=9),
                     lambda run: run["maintenance_jobs"][0].update(batches=True),
                     lambda run: run["maintenance_job_audit"].update(committed_batches=17),
                     lambda run: run["store_metrics"].update(commits=run["completed_messages"]),
                     lambda run: run.update(completed_transactions=run["completed_messages"]),
                     lambda run: run.update(completed_messages=run["completed_transactions"]),
                     lambda run: run["transaction_kinds"].update(global_projection=4),
                     lambda run: run["per_kind"]["global_projection"].update(transactions=4),
                     lambda run: run["message_kinds"].update(global_projection=12),
                     lambda run: run["per_kind"]["global_projection"].update(completed=12)]
        for index, mutate in enumerate(mutations):
            with self.subTest(index=index):
                item = sweep_trial()
                mutate(item["report"]["runs"][0])
                self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])
        item = sweep_trial(max_maintenance_batches=2)
        self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])

    def test_sweep_admission_effects_and_whole_job_latencies_are_independently_audited(self):
        mutations = [lambda run: run["maintenance_jobs"][0].update(worker=0),
                     lambda run: run["maintenance_jobs"][0].update(job_ordinal=1),
                     lambda run: run["maintenance_jobs"][0].update(job_id=4000000001),
                     lambda run: run["maintenance_jobs"][0].update(scheduled_ns=2000000001),
                     lambda run: run["maintenance_jobs"][0].update(started_ns=1999999999),
                     lambda run: run["maintenance_jobs"][0].update(received_ns=9000000000),
                     lambda run: run["maintenance_jobs"][0].update(first_transaction_id=8000000001),
                     lambda run: run["maintenance_jobs"][0].update(terminal_transaction_id=8000000001),
                     lambda run: run["maintenance_jobs"][0]["outcomes"].update(claimed_events=4),
                     lambda run: run["per_kind"]["global_projection"]["outcomes"].update(outputs=1),
                     lambda run: run["workload_classes"]["projection"].update(p99_us_including_retries=1000),
                     lambda run: run["workload_classes"]["maintenance"].update(service_latency_p99_us_including_retries=1000),
                     lambda run: run["workload_classes"]["housekeeping"].update(arrival_queue_delay_p99_us=1),
                     lambda run: run["worker_activity"][2].update(busy_ns=1),
                     lambda run: run["maintenance_jobs"][0].update(retries=2)]
        for index, mutate in enumerate(mutations):
            with self.subTest(index=index):
                item = sweep_trial()
                mutate(item["report"]["runs"][0])
                self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])

    def test_empty_sweep_and_no_scheduled_jobs_do_not_invent_positive_work(self):
        empty = gate.assess_trial(sweep_trial(empty=True, max_maintenance_batches=1), POLICY)
        self.assertTrue(empty["execution_valid"], empty)
        self.assertTrue(empty["global_maintenance_sweep_complete"])
        self.assertEqual(empty["maintenance_jobs"]["projection"]["positive_effect_jobs"], 0)
        short = gate.assess_trial(sweep_trial(seconds=1), POLICY)
        self.assertTrue(short["execution_valid"], short)
        self.assertFalse(short["global_maintenance_sweep_complete"])
        for mutate in (lambda run: run["maintenance_jobs"][0]["outcomes"].update(outputs=1),
                       lambda run: run["maintenance_jobs"].__setitem__(0, None),
                       lambda run: run["per_kind"].__setitem__("global_projection", None),
                       lambda run: run["per_kind"]["global_projection"].update(outcomes=None),
                       lambda run: run["maintenance_job_audit"].update(terminal_batches=True)):
            item = sweep_trial(empty=True)
            mutate(item["report"]["runs"][0])
            self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])

    def test_maintenance_companions_require_identical_options_with_legacy_default_normalization(self):
        legacy = calibrated_trial()
        explicit = copy.deepcopy(legacy)
        explicit["config"].update(gate.MAINTENANCE_DEFAULTS)
        explicit["report"]["config"].update(gate.MAINTENANCE_DEFAULTS)
        self.assertEqual(gate.key(legacy["config"]), gate.key(explicit["config"]))
        self.assertTrue(gate.assess_trial(explicit, POLICY)["execution_valid"])
        item = sweep_trial(evidence="metrics")
        own = {gate.key(item["config"])}
        verdict = gate.assess_trial(item, POLICY, own)
        self.assertTrue(verdict["correctness_companion_verified"])
        self.assertFalse(verdict["history_verified"])
        for field, value in (("maintenance_mode", "batch"), ("projection_batch_size", 8),
                             ("housekeeping_batch_size", 64), ("max_maintenance_batches", 4095)):
            with self.subTest(field=field):
                other = {**item["config"], field: value}
                self.assertNotEqual(gate.key(item["config"]), gate.key(other))
                self.assertFalse(gate.assess_trial(item, POLICY, {gate.key(other)})["correctness_companion_verified"])

    def test_signature_affinity_sliding_ttl_and_expiry_equality_have_golden_routes(self):
        config = calibrated_trial(seconds=1, rate=10, families=4, workers=2)["config"]
        config.update(dispatch="signature-affinity", affinity_ttl_ms=300, signature_pattern="both")
        expired = gate.calibrated_dispatch(config)
        # A three-flight cycle takes exactly300ms. Equality expires every entry;
        # each miss advances the two-worker rotor, flipping seven old owners.
        self.assertEqual({k: expired[k] for k in ("hits", "misses", "new_signature_misses", "expired_misses", "expired_owner_changes", "planned_flight_worker_changes", "flights_with_multiple_workers")},
                         dict(hits=0, misses=10, new_signature_misses=3, expired_misses=7,
                              expired_owner_changes=7, planned_flight_worker_changes=7, flights_with_multiple_workers=3))
        self.assertEqual(expired["worker_counts"], [5, 5])
        self.assertEqual(expired["assignment_fingerprint"], "5e9651c1c8fb3a05")
        refreshed = gate.calibrated_dispatch({**config, "affinity_ttl_ms": 301})
        self.assertEqual((refreshed["hits"], refreshed["misses"], refreshed["expired_misses"]), (7, 3, 0))
        self.assertEqual(refreshed["worker_counts"], [7, 3])
        self.assertEqual(refreshed["assignment_fingerprint"], "b33157edbe18c145")
        self.assertEqual(refreshed["planned_flight_worker_changes"], 0)

    def test_signature_alias_pattern_has_shared_callsign_hits_and_distinct_tail_keys(self):
        config = calibrated_trial(seconds=3, rate=6, families=4, workers=2)["config"]
        config.update(dispatch="signature-affinity", affinity_ttl_ms=10000, signature_pattern="mixed")
        audit = gate.calibrated_dispatch(config)
        # Per-flight aliases: both,both,callsign,callsign,tail,tail. All three
        # flights share callsign100; destinations still distinguish DB matches.
        self.assertEqual((audit["unique_signatures"], audit["hits"], audit["misses"]), (7, 11, 7))
        self.assertEqual(audit["worker_counts"], [8, 10])
        self.assertEqual(audit["planned_flight_worker_changes"], 4)
        self.assertEqual(audit["flights_with_multiple_workers"], 2)
        self.assertEqual(audit["assignment_fingerprint"], "5226764c10a68564")
        identity = gate.calibrated_dispatch({**config, "dispatch": "identity", "affinity_ttl_ms": 0})
        self.assertEqual(identity["unique_signatures"], 7)
        self.assertEqual(identity["worker_counts"], [12, 6])
        self.assertEqual(identity["hits"] + identity["misses"] + identity["planned_flight_worker_changes"], 0)

    def test_affinity_valid_reordering_and_partly_stale_updates_remain_history_valid(self):
        evidence = routed_trial(seconds=3, rate=6, families=4, workers=2)
        run = evidence["report"]["runs"][0]
        run["per_flight_order"].update(passed=False, overlapping_messages=2, out_of_order_completions=1)
        run["per_kind"]["position"]["outcomes"]["ignored_stale"] = 1
        run["per_kind"]["position"]["outcomes"]["updated_views"] -= 1
        result = gate.assess_trial(evidence, POLICY)
        self.assertTrue(result["execution_valid"], result["reasons"])
        self.assertTrue(result["history_verified"])
        self.assertFalse(result["foreground_ordering_required"])
        self.assertFalse(result["foreground_ordering_passed"])
        self.assertEqual(result["foreground_positive_job_fraction"], 1)
        self.assertGreater(result["foreground_stale_view_update_fraction"], 0)
        for field in ("useful_work_passed", "foreground_effect_coverage_passed", "diagnostic_performance_passed", "qualified_capacity_trial", "capacity_failure"):
            self.assertFalse(result[field], field)

    def test_affinity_requires_exact_dispatch_and_consistent_order_diagnostics(self):
        mutations = [lambda run: run.pop("dispatch_audit"),
                     lambda run: run["dispatch_audit"].update(hits=999),
                     lambda run: run["dispatch_audit"].update(assignment_fingerprint="0000000000000000"),
                     lambda run: run["dispatch_audit"].update(clock="completion_time"),
                     lambda run: run["dispatch_audit"].update(worker_counts=[0, 18]),
                     lambda run: run["per_flight_order"].update(required=True),
                     lambda run: run["per_flight_order"].update(passed=False),
                     lambda run: run["per_flight_order"].update(overlapping_messages=-1)]
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                evidence = routed_trial(seconds=3, rate=6, families=4, workers=2)
                mutate(evidence["report"]["runs"][0])
                self.assertFalse(gate.assess_trial(evidence, POLICY)["execution_valid"])

    def test_dispatch_defaults_preserve_old_reports_and_companions_require_all_parameters(self):
        old = calibrated_trial(seconds=1, rate=10, families=4, workers=2)
        explicit = copy.deepcopy(old)
        explicit["config"].update(gate.DISPATCH_DEFAULTS)
        self.assertEqual(gate.key(old["config"]), gate.key(explicit["config"]))
        self.assertTrue(gate.assess_trial(explicit, POLICY)["history_verified"])
        evidence = routed_trial(seconds=3, rate=6, families=4, workers=2, evidence="metrics")
        config = evidence["config"]
        self.assertTrue(gate.assess_trial(evidence, POLICY, {gate.key(config)})["correctness_companion_verified"])
        for changes in ({"affinity_ttl_ms": 1001}, {"signature_pattern": "both"}, {"dispatch": "identity", "affinity_ttl_ms": 0}):
            with self.subTest(changes=changes):
                self.assertFalse(gate.assess_trial(evidence, POLICY, {gate.key({**config, **changes})})["correctness_companion_verified"])
        # A declared identity request cannot accept an affinity report even
        # when both happen to have identical per-worker completion counts.
        explicit["report"]["config"].update(dispatch="signature-affinity", affinity_ttl_ms=301)
        self.assertFalse(gate.assess_trial(explicit, POLICY)["execution_valid"])

    def test_mixed_identity_control_keeps_strict_fifo_and_matrices_do_not_mix_dispatch(self):
        control = routed_trial(dispatch="identity", ttl=0, seconds=3, rate=6, families=4, workers=2)
        self.assertTrue(gate.assess_trial(control, POLICY)["execution_valid"])
        failed = copy.deepcopy(control)
        failed["report"]["runs"][0]["per_flight_order"]["passed"] = False
        self.assertFalse(gate.assess_trial(failed, POLICY)["execution_valid"])
        other = routed_trial(seconds=3, rate=6, families=4, workers=2)
        result = gate.build_gate([control, other], POLICY, ["aerostore"], [6], [2], [11])
        self.assertFalse(result["corpus_configuration_matches"])

    def test_invalid_dispatch_cli_is_rejected_before_running_a_benchmark(self):
        for flags in (["--dispatch", "signature-affinity"], ["--affinity-ttl-ms", "1"],
                      ["--dispatch", "signature-affinity", "--affinity-ttl-ms", "-1"],
                      ["--dispatch", "signature-affinity", "--affinity-ttl-ms", "3600001"],
                      ["--workload", "fleet", "--signature-pattern", "mixed"]):
            with self.subTest(flags=flags), tempfile.TemporaryDirectory() as directory:
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as caught:
                    gate.main(["--binary", "/nonexistent/benchmark", "--output", directory,
                               "--workload", "calibrated", "--families", "16", "--hot-percent", "0",
                               "--slo-ms", "50", *flags])
                self.assertEqual(caught.exception.code, 2)

    def test_calibrated_corpus_counts_and_exclusive_timer_endpoints(self):
        config = calibrated_trial(seconds=7, rate=12, projection=2, housekeeping=3)["config"]
        corpus = gate.calibrated_corpus(config)
        self.assertEqual(corpus["worker_counts"], [21, 21, 21, 21, 3, 2])
        self.assertEqual(corpus["kinds"], {"plan": 12, "position": 72, "global_projection": 3, "global_housekeeping": 2})
        self.assertEqual(corpus["total"], 89)
        for seconds, expected in [(1, (0, 0)), (300, (0, 0)), (301, (1, 0)),
                                  (600, (1, 0)), (601, (2, 1)), (1201, (4, 2))]:
            with self.subTest(seconds=seconds):
                self.assertEqual(gate.calibrated_timer_counts({**config, "seconds": seconds,
                                 "projection_interval_seconds": 300, "housekeeping_interval_seconds": 600}), expected)
        # Uneven identity lanes cannot be approximated by N / workers.
        corpus = gate.calibrated_corpus({**config, "families": 17, "seconds": 2, "arrival_rate": 13})
        self.assertEqual(corpus["worker_counts"], [8, 6, 6, 6, 0, 0])
        self.assertEqual(corpus["kinds"], {"plan": 13, "position": 13})

    def test_calibrated_short_run_validates_history_without_inventing_maintenance(self):
        evidence = calibrated_trial(seconds=2, rate=12)
        verdict = gate.assess_trial(evidence, POLICY)
        self.assertTrue(verdict["execution_valid"], verdict["reasons"])
        self.assertTrue(verdict["history_verified"])
        self.assertTrue(verdict["useful_work_passed"])
        self.assertTrue(verdict["diagnostic_performance_passed"])
        self.assertTrue(verdict["representative_cadence_config"])
        self.assertFalse(verdict["representative_cadence_coverage_passed"])
        self.assertFalse(verdict["qualified_capacity_trial"])
        self.assertFalse(verdict["capacity_failure"])
        self.assertEqual(verdict["outcomes"]["created_views"], 0)
        self.assertEqual(verdict["outcomes"]["expired_families"], 0)
        for name in ("projection", "housekeeping"):
            self.assertEqual(verdict["maintenance_jobs"][name]["completed"], 0)

    def test_accelerated_timers_cannot_claim_operator_cadence(self):
        verdict = gate.assess_trial(calibrated_trial(seconds=7, rate=12, projection=2, housekeeping=3), POLICY)
        self.assertTrue(verdict["execution_valid"], verdict["reasons"])
        self.assertTrue(verdict["history_verified"])
        self.assertTrue(verdict["repeated_positive_maintenance_jobs"])
        self.assertFalse(verdict["representative_cadence_config"])
        self.assertFalse(verdict["representative_cadence_coverage_passed"])
        self.assertFalse(verdict["qualified_capacity_trial"])

    def test_real_cadence_still_cannot_qualify_steady_state_bounded_batches(self):
        evidence = calibrated_trial()
        verdict = gate.assess_trial(evidence, POLICY)
        self.assertTrue(verdict["execution_valid"], verdict["reasons"])
        self.assertTrue(verdict["representative_cadence_coverage_passed"])
        self.assertTrue(verdict["diagnostic_performance_passed"])
        for flag in ("population_turnover_tested", "global_maintenance_sweep_complete",
                     "calibrated_capacity_qualification_complete", "qualified_capacity_trial", "capacity_failure"):
            self.assertFalse(verdict[flag], flag)
        high = calibrated_trial(p99_ms=999)
        self.assertFalse(gate.assess_trial(high, POLICY)["capacity_failure"], "unqualified profile cannot provide a saturation upper bound")
        trials = []
        for seed in SEEDS:
            copied = copy.deepcopy(evidence)
            copied["config"]["seed"] = copied["report"]["config"]["seed"] = seed
            trials.append(copied)
        grid = gate.build_gate(trials, POLICY, ["aerostore"], [1], [4], SEEDS)
        self.assertIsNone(grid["capacity_bounds"]["aerostore"]["best_tested_qualified_rate"])
        self.assertIsNone(grid["capacity_bounds"]["aerostore"]["best_tested_exploratory_rate"])

    def test_cadence_coverage_needs_two_positive_jobs_not_just_two_empty_queries(self):
        for seconds in (301, 601):
            verdict = gate.assess_trial(calibrated_trial(seconds=seconds), POLICY)
            self.assertTrue(verdict["execution_valid"])
            self.assertFalse(verdict["representative_cadence_coverage_passed"])
        evidence = calibrated_trial()
        classes = evidence["report"]["runs"][0]["workload_classes"]
        classes["housekeeping"]["positive_effect_jobs"] = 1
        classes["maintenance"]["positive_effect_jobs"] = classes["projection"]["positive_effect_jobs"] + 1
        verdict = gate.assess_trial(evidence, POLICY)
        self.assertTrue(verdict["execution_valid"])
        self.assertFalse(verdict["representative_cadence_coverage_passed"])

    def test_calibrated_incomplete_jobs_bad_order_or_wrong_lane_counts_fail_execution(self):
        mutations = [
            lambda run: run["calibrated_schedule"].update(first_timer_tick="at_start"),
            lambda run: run["calibrated_schedule"].update(timer_admission="including_end"),
            lambda run: run["calibrated_schedule"].update(clock="accelerated_event_time"),
            lambda run: run["calibrated_schedule"].update(cadence="accelerated"),
            lambda run: run["calibrated_schedule"].update(maintenance_scope="full_sweep"),
            lambda run: run["calibrated_schedule"].update(population_turnover_tested=True),
            lambda run: run["per_flight_order"].update(passed=False),
            lambda run: run["per_flight_order"].update(identities_observed=16),
            lambda run: run["workload_classes"]["projection"].update(completed=3),
            lambda run: run["workload_classes"]["maintenance"].update(retries=1),
            lambda run: run["worker_activity"][4].update(role="foreground"),
            lambda run: run["worker_activity"][0].update(utilization=.99),
            lambda run: run.update(completed_by_worker=[300, 301, 300, 300, 4, 2]),
            lambda run: run.update(total_process_workers=4),
            lambda run: run.update(offered_rate_scope="foreground_and_background"),
        ]
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                evidence = calibrated_trial()
                mutate(evidence["report"]["runs"][0])
                self.assertFalse(gate.assess_trial(evidence, POLICY)["execution_valid"])
        evidence = calibrated_trial(seconds=2)
        evidence["report"]["runs"][0]["workload_classes"]["housekeeping"]["p99_us_including_retries"] = 0
        self.assertFalse(gate.assess_trial(evidence, POLICY)["execution_valid"], "empty jobs do not have measured p99")

    def test_calibrated_metrics_companion_requires_exact_cadence_and_corpus(self):
        evidence = calibrated_trial(evidence="metrics")
        verdict = gate.assess_trial(evidence, POLICY)
        self.assertTrue(verdict["execution_valid"])
        self.assertFalse(verdict["correctness_companion_verified"])
        verdict = gate.assess_trial(evidence, POLICY, {gate.key(evidence["config"])})
        self.assertTrue(verdict["correctness_companion_verified"])
        self.assertFalse(verdict["history_verified"])
        self.assertFalse(verdict["qualified_capacity_trial"])
        for field in gate.CALIBRATED_FIELDS:
            wrong = copy.deepcopy(evidence["config"])
            wrong[field] += 1
            self.assertFalse(gate.assess_trial(evidence, POLICY, {gate.key(wrong)})["correctness_companion_verified"])
        old = trial()["config"]
        self.assertEqual(gate.key(old), gate.key({**old, "projection_interval_seconds": 1,
                                                 "housekeeping_interval_seconds": 2}), "old stress keys stay unchanged")

    def test_calibrated_matrix_cannot_mix_timer_cadences(self):
        trials = [calibrated_trial(projection=interval) for interval in (300, 600)]
        grid = gate.build_gate(trials, POLICY, ["aerostore"], [1], [4], [11])
        self.assertFalse(grid["corpus_configuration_matches"])
        self.assertTrue(all(not item["assessment"]["execution_valid"] for item in grid["trial_assessments"]))

    def test_calibrated_fast_skipped_updates_fail_usefulness_but_preserve_valid_history(self):
        for field in ("missing_family", "allocation_deferred", "ignored_stale", "duplicate_messages"):
            with self.subTest(outcome=field):
                evidence = calibrated_trial()
                evidence["report"]["runs"][0]["per_kind"]["plan"]["outcomes"][field] = 1
                verdict = gate.assess_trial(evidence, POLICY)
                self.assertTrue(verdict["execution_valid"])
                self.assertTrue(verdict["history_verified"])
                self.assertFalse(verdict["foreground_effect_coverage_passed"])
                self.assertFalse(verdict["diagnostic_performance_passed"])
        evidence = calibrated_trial()
        evidence["report"]["runs"][0]["workload_classes"]["foreground"]["positive_effect_jobs"] -= 1
        verdict = gate.assess_trial(evidence, POLICY)
        self.assertTrue(verdict["history_verified"])
        self.assertFalse(verdict["useful_work_passed"])
        self.assertFalse(verdict["diagnostic_performance_passed"])

    def test_calibrated_cli_rejects_lane_or_timer_caps_and_invalid_intervals(self):
        changes = [
            ["--projection-interval-seconds", "0"], ["--housekeeping-interval-seconds", "3601"],
            ["--workers", "33"], ["--families", "17", "--rates", "100000", "--seconds", "4"],
            ["--rates", "1", "--seconds", "3600", "--projection-interval-seconds", "1", "--max-messages", "1000"],
        ]
        for extra in changes:
            with self.subTest(arguments=extra), tempfile.TemporaryDirectory() as directory:
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    gate.main(["--binary", "/missing", "--output", directory, "--engines", "aerostore",
                               "--workload", "calibrated", "--hot-percent", "0", "--workers", "4", "--slo-ms", "100", *extra])
                self.assertEqual(json.loads((Path(directory) / "campaign.json").read_text())["stage"], "configuration")

    def test_calibrated_idle_foreground_lanes_remain_explicitly_idle(self):
        evidence = calibrated_trial(seconds=1, rate=1, workers=32, families=4)
        expected = gate.calibrated_corpus(evidence["config"])["worker_counts"]
        self.assertEqual(expected, [1] + [0] * 33)
        verdict = gate.assess_trial(evidence, POLICY)
        self.assertTrue(verdict["execution_valid"], verdict["reasons"])
        self.assertTrue(verdict["history_verified"])
        activity = evidence["report"]["runs"][0]["worker_activity"]
        self.assertTrue(all(worker["busy_ns"] == 0 and worker["utilization"] == 0 for worker in activity[1:]))
        activity[1]["busy_ns"] = 1
        activity[1]["utilization"] = 1 / 1_005_000_000
        self.assertFalse(gate.assess_trial(evidence, POLICY)["execution_valid"])

    def test_legacy_timing_preserves_history_but_cannot_qualify_capacity_or_saturation(self):
        for p99_ms in (1, 999):
            evidence = trial(p99_ms=p99_ms)
            run = evidence["report"]["runs"][0]
            for name in ("admission_started_ns", "workload_completed_ns", "workers_stopped_ns", "drain_confirmed_ns",
                         "elapsed_seconds_including_drain", "timing_scope"):
                del run[name]
            verdict = gate.assess_trial(evidence, POLICY)
            self.assertTrue(verdict["execution_valid"])
            self.assertTrue(verdict["history_verified"])
            self.assertTrue(verdict["correctness_companion_verified"])
            self.assertTrue(verdict["useful_work_passed"])
            self.assertFalse(verdict["continuous_timing_passed"])
            self.assertFalse(verdict["performance_passed"])
            self.assertFalse(verdict["qualified_capacity_trial"])
            self.assertFalse(verdict["capacity_failure"])

    def test_legacy_three_seed_grid_cannot_supply_passing_or_saturation_bounds(self):
        trials = self.saturation_trials()
        for evidence in trials:
            evidence["report"]["runs"][0].pop("timing_scope")
        result = gate.build_gate(trials, POLICY, ["aerostore", "postgres"], [32, 64, 640], [1, 2], SEEDS)
        for bound in result["capacity_bounds"].values():
            self.assertIsNone(bound["best_tested_qualified_rate"])
            self.assertIsNone(bound["best_tested_exploratory_rate"])
            self.assertIsNone(bound["failing_tested_upper_rate"])
        self.assertIsNone(result["comparisons"]["aerostore"]["synthetic_capacity_lower_bound"])

    def test_continuous_timing_rejects_bad_clock_order_denominator_and_reported_rate(self):
        changes = [
            {"timing_scope": "server_monotonic"},
            {"admission_started_ns": True},
            {"workload_completed_ns": 1_000_000_000},
            {"workers_stopped_ns": 6_000_000_000},
            {"drain_confirmed_ns": 6_002_000_000},
            {"drain_confirmed_ns": 2**64},
            {"elapsed_seconds": 5.000},
            {"elapsed_seconds_including_drain": 5.001},
            {"elapsed_seconds_including_drain": float("nan")},
            {"completed_messages_per_second_including_drain": 32},
        ]
        for change in changes:
            with self.subTest(change=change):
                evidence = trial()
                evidence["report"]["runs"][0].update(change)
                verdict = gate.assess_trial(evidence, POLICY)
                self.assertTrue(verdict["execution_valid"])
                self.assertTrue(verdict["history_verified"])
                self.assertFalse(verdict["continuous_timing_passed"])
                self.assertFalse(verdict["qualified_capacity_trial"])
                self.assertFalse(verdict["capacity_failure"])

    def test_worker_shutdown_delay_is_included_in_capacity_denominator(self):
        evidence = trial()
        run = evidence["report"]["runs"][0]
        # Work completes at five seconds, but a slow worker shutdown delays the
        # confirmed drain until ten seconds after admission. It must not get
        # free WAL-flush time between separately measured workload/drain phases.
        run.update(workers_stopped_ns=10_999_000_000, drain_confirmed_ns=11_000_000_000,
                   elapsed_seconds_including_drain=10.0,
                   completed_messages_per_second_including_drain=16.0)
        verdict = gate.assess_trial(evidence, POLICY)
        self.assertTrue(verdict["continuous_timing_passed"])
        self.assertTrue(verdict["history_verified"])
        self.assertFalse(verdict["qualified_capacity_trial"])
        self.assertTrue(verdict["capacity_failure"])
        # The old sum of five workload seconds plus a millisecond of drain is
        # inconsistent with these timestamps, even if used in both scalars.
        run.update(elapsed_seconds_including_drain=5.002,
                   completed_messages_per_second_including_drain=160 / 5.002)
        verdict = gate.assess_trial(evidence, POLICY)
        self.assertFalse(verdict["continuous_timing_passed"])
        self.assertFalse(verdict["qualified_capacity_trial"])
        self.assertFalse(verdict["capacity_failure"])

    def test_fleet_kind_counts_match_independently_expected_prefix_and_full_rounds(self):
        config = {"workload": "fleet", "families": 16, "seed": 20260925, "hot_percent": 0}
        self.assertEqual(gate.expected_kind_counts(config, 7), {"position": 1, "global_projection": 1,
                          "global_reschedule": 1, "arrival": 3, "expire_family": 1})
        for families in [16, 17, 32]:
            config["families"] = families
            self.assertEqual(gate.expected_kind_counts(config, 16 * families), {
                "plan": 3 * families, "position": 3 * families, "arrival": 3 * families,
                "global_projection": families, "global_reschedule": families, "global_cancel": families,
                "global_housekeeping": 2 * families, "expire_family": 2 * families})

    def test_fleet_requires_population_and_mutating_background_work(self):
        evidence = fleet_trial()
        verdict = gate.assess_trial(evidence, POLICY)
        self.assertTrue(verdict["qualified_capacity_trial"])
        self.assertEqual(verdict["configured_identities"], 16)
        self.assertEqual(verdict["initial_observed_live_families"], 11)
        self.assertIn("not a runtime minimum", verdict["population_scope"])
        for snapshot in ["initial_fleet", "final_fleet"]:
            bad = copy.deepcopy(evidence)
            bad["report"]["runs"][0][snapshot]["live_families"] = 1
            verdict = gate.assess_trial(bad, POLICY)
            self.assertTrue(verdict["execution_valid"])
            self.assertFalse(verdict["qualified_capacity_trial"])
            self.assertFalse(verdict["capacity_failure"])
        for kind, outcome in [("global_projection", "claimed_events"), ("global_reschedule", "rescheduled_events"),
                              ("global_cancel", "cancelled_events"), ("global_housekeeping", "expired_records")]:
            bad = copy.deepcopy(evidence)
            bad["report"]["runs"][0]["per_kind"][kind]["outcomes"][outcome] = 0
            verdict = gate.assess_trial(bad, POLICY)
            self.assertTrue(verdict["execution_valid"])
            self.assertFalse(verdict["qualified_capacity_trial"])
            self.assertFalse(verdict["capacity_failure"])
        bad = copy.deepcopy(evidence)
        bad["report"]["runs"][0]["message_kinds"] = gate.lifecycle_kind_counts(325)
        self.assertFalse(gate.assess_trial(bad, POLICY)["execution_valid"])

    def test_fleet_requires_a_complete_phase_cycle_and_explicit_uniform_policy(self):
        config = fleet_trial()["config"]
        for field, value in [("families", 15), ("hot_percent", 80)]:
            wrong = {**config, field: value}
            with self.assertRaises(ValueError):
                gate.expected_kind_counts(wrong, 325)
        evidence = fleet_trial()
        for cfg in [evidence["config"], evidence["report"]["config"]]:
            cfg["families"] = 32
        run = evidence["report"]["runs"][0]
        counts = gate.expected_kind_counts(evidence["config"], 325)
        run["message_kinds"] = counts
        for kind, count in counts.items():
            run["per_kind"][kind]["completed"] = count
        run["initial_fleet"]["live_families"] = 21
        run["final_fleet"]["live_families"] = 21
        verdict = gate.assess_trial(evidence, POLICY)
        self.assertTrue(verdict["execution_valid"])
        self.assertFalse(verdict["fleet_complete_phase_cycle"])
        self.assertFalse(verdict["qualified_capacity_trial"])

    def test_full_history_has_qualified_test_capacity_without_engine_promotion(self):
        verdict = gate.assess_trial(trial(), POLICY)
        self.assertTrue(verdict["history_verified"])
        self.assertTrue(verdict["qualified_capacity_trial"])
        result = gate.build_gate([trial(seed=seed) for seed in SEEDS], POLICY, ["aerostore"], [32], [1], SEEDS)
        self.assertEqual(result["capacity_bounds"]["aerostore"]["best_tested_qualified_rate"], 32)
        self.assertFalse(result["whole_engine_verified"])
        self.assertFalse(result["actual_hyperfeed_replacement_qualified"])
        self.assertFalse(result["architecture_promotion_eligible"])

    def test_metrics_alone_cannot_claim_history_or_qualified_capacity(self):
        evidence = trial(evidence="metrics")
        verdict = gate.assess_trial(evidence, POLICY)
        self.assertTrue(verdict["execution_valid"])
        self.assertTrue(verdict["performance_passed"])
        self.assertFalse(verdict["history_verified"])
        self.assertFalse(verdict["qualified_capacity_trial"])
        verdict = gate.assess_trial(evidence, POLICY, {gate.key(evidence["config"])})
        self.assertTrue(verdict["qualified_capacity_trial"])
        self.assertFalse(verdict["history_verified"], "a companion never verifies an unrecorded run")

    def test_companion_requires_exact_source_binary_and_complete_capture(self):
        source = {"sha256": "source", "files": {"dirty.rs": "hash"}}
        companion = {"format": gate.FORMAT, "completed": True, "source_stable": True,
                     "source_before": source, "source_after": copy.deepcopy(source),
                     "binary_before_sha256": "binary", "binary_after_sha256": "binary"}
        self.assertTrue(gate.compatible_companion(companion, source, "binary"))
        for field, value in (("completed", False), ("source_stable", False),
                             ("source_after", {}), ("binary_after_sha256", "other")):
            bad = {**companion, field: value}
            self.assertFalse(gate.compatible_companion(bad, source, "binary"))
        self.assertFalse(gate.compatible_companion({"passed": True, "git_revision": "same"}, source, "binary"))

    def test_companion_corpus_and_worker_configuration_cannot_be_substituted(self):
        evidence = trial(evidence="metrics")
        for field in gate.MATCH_FIELDS:
            wrong = copy.deepcopy(evidence["config"])
            wrong[field] = str(wrong[field]) + "different"
            with self.subTest(field=field):
                self.assertFalse(gate.assess_trial(evidence, POLICY, {gate.key(wrong)})["qualified_capacity_trial"])

    def test_errors_timeouts_source_drift_and_progress_failures_are_not_saturation(self):
        for change in ({"exit_code": 1}, {"timed_out": True}, {"source_stable": False}, {"error": "128 retries"}):
            evidence = trial(p99_ms=999)
            evidence.update(change)
            verdict = gate.assess_trial(evidence, POLICY)
            self.assertFalse(verdict["execution_valid"])
            self.assertFalse(verdict["capacity_failure"])
        evidence = trial(p99_ms=999)
        evidence["report"]["runs"][0].update(passed=False, error="no worker progress")
        self.assertFalse(gate.assess_trial(evidence, POLICY)["capacity_failure"])

    def test_wrong_or_incomplete_corpus_is_rejected(self):
        changes = [{"completed_messages": 159}, {"offered_messages": 159},
                   {"completed_by_worker": [159]}, {"arrival_mode": "closed_loop"},
                   {"requested_duration_reached": False}, {"elapsed_seconds": .1},
                   {"worker_message_cap_reached": True}, {"admission_seconds": 4},
                   {"worker_stops": [{"stop_reason": "message_cap"}]},
                   {"message_kinds": {"plan": 160}}, {"store_metrics": {"commits": 159}}]
        for change in changes:
            evidence = trial()
            evidence["report"]["runs"][0].update(change)
            with self.subTest(change=change):
                self.assertFalse(gate.assess_trial(evidence, POLICY)["execution_valid"])
        evidence = trial()
        evidence["report"]["config"]["seed"] += 1
        self.assertFalse(gate.assess_trial(evidence, POLICY)["execution_valid"])

    def test_invalid_or_inconclusive_history_never_qualifies(self):
        for status in ("Invalid", "Inconclusive", "NotCheckedMetricsOnly"):
            evidence = trial()
            evidence["report"]["runs"][0]["oracle_status"] = status
            self.assertFalse(gate.assess_trial(evidence, POLICY)["qualified_capacity_trial"])
        evidence = trial(evidence="metrics")
        evidence["report"]["runs"][0]["history_checked"] = True
        self.assertFalse(gate.assess_trial(evidence, POLICY)["execution_valid"])

    def test_useful_work_collapse_cannot_qualify_capacity(self):
        for field, value in (("missing_family", 100), ("allocation_deferred", 100), ("created_views", 0), ("expired_families", 0)):
            evidence = trial()
            evidence["report"]["runs"][0]["per_kind"]["plan"]["outcomes"][field] = value
            with self.subTest(field=field):
                verdict = gate.assess_trial(evidence, POLICY)
                self.assertTrue(verdict["execution_valid"])
                self.assertFalse(verdict["performance_passed"])
                self.assertFalse(verdict["capacity_failure"])

    def test_every_seed_must_pass_and_one_seed_cannot_establish_repeated_capacity(self):
        trials = [trial(seed=seed) for seed in SEEDS]
        trials[-1]["report"]["runs"][0]["message_latency_p99_us_including_retries"] = 999000
        result = gate.build_gate(trials, POLICY, ["aerostore"], [32], [1], SEEDS)
        self.assertIsNone(result["capacity_bounds"]["aerostore"]["best_tested_qualified_rate"])
        result = gate.build_gate([trial()], POLICY, ["aerostore"], [32], [1], [11])
        self.assertIsNone(result["capacity_bounds"]["aerostore"]["best_tested_qualified_rate"])

    def test_two_passing_lower_bounds_do_not_prove_tenfold_capacity(self):
        trials = [trial(engine=engine, rate=rate, seed=seed) for engine, rate in (("aerostore", 320), ("postgres", 32)) for seed in SEEDS]
        result = gate.build_gate(trials, POLICY, ["aerostore", "postgres"], [32, 320], [1], SEEDS)
        comparison = result["comparisons"]["aerostore"]
        self.assertEqual(comparison["ratio_of_tested_passing_rates"], 10)
        self.assertIsNone(comparison["synthetic_capacity_lower_bound"])
        self.assertFalse(comparison["synthetic_10x_bound_demonstrated"])

    def saturation_trials(self):
        return [trial(engine=engine, rate=rate, workers=workers, seed=seed,
                      p99_ms=999 if engine == "postgres" and rate >= 64 else 1)
                for engine in ("aerostore", "postgres") for rate in (32, 64, 640)
                for workers in (1, 2) for seed in SEEDS]

    def test_saturation_requires_all_worker_choices_and_seed_failures(self):
        trials = self.saturation_trials()
        result = gate.build_gate(trials, POLICY, ["aerostore", "postgres"], [32, 64, 640], [1, 2], SEEDS)
        self.assertEqual(result["capacity_bounds"]["postgres"]["failing_tested_upper_rate"], 64)
        self.assertEqual(result["capacity_bounds"]["aerostore"]["selected_workers"], 1)
        self.assertEqual(result["comparisons"]["aerostore"]["synthetic_capacity_lower_bound"], 10)
        self.assertTrue(result["comparisons"]["aerostore"]["conditional_tested_grid_10x_ratio"])
        self.assertFalse(result["comparisons"]["aerostore"]["synthetic_10x_bound_demonstrated"])
        self.assertFalse(result["comparisons"]["aerostore"]["hardware_resource_equivalence_verified"])
        self.assertFalse(result["real_world_10x_claim"])
        for item in trials:
            if item["config"]["engine"] == "postgres" and item["config"]["workers"] == 2 and item["config"]["arrival_rate"] >= 64:
                item["exit_code"] = 1
        result = gate.build_gate(trials, POLICY, ["aerostore", "postgres"], [32, 64, 640], [1, 2], SEEDS)
        self.assertIsNone(result["capacity_bounds"]["postgres"]["failing_tested_upper_rate"])
        self.assertFalse(result["comparisons"]["aerostore"]["synthetic_10x_bound_demonstrated"])

    def test_saturation_cannot_hide_mismatched_useful_work_at_high_rate(self):
        trials = self.saturation_trials()
        for item in trials:
            if item["config"]["engine"] == "aerostore" and item["config"]["arrival_rate"] == 640:
                item["report"]["runs"][0]["per_kind"]["plan"]["outcomes"]["updated_views"] //= 2
        result = gate.build_gate(trials, POLICY, ["aerostore", "postgres"], [32, 64, 640], [1, 2], SEEDS)
        self.assertFalse(result["comparisons"]["aerostore"]["useful_work_comparable"])
        self.assertIsNone(result["comparisons"]["aerostore"]["synthetic_capacity_lower_bound"])

    def test_different_engine_corpora_cannot_be_compared_as_one_matrix(self):
        trials = self.saturation_trials()
        for item in trials:
            if item["config"]["engine"] == "aerostore":
                item["config"]["families"] = 1
                item["report"]["config"]["families"] = 1
        result = gate.build_gate(trials, POLICY, ["aerostore", "postgres"], [32, 64, 640], [1, 2], SEEDS)
        self.assertFalse(result["corpus_configuration_matches"])
        self.assertIsNone(result["capacity_bounds"]["aerostore"]["best_tested_qualified_rate"])
        self.assertIsNone(result["comparisons"]["aerostore"]["synthetic_capacity_lower_bound"])

    def test_missing_and_duplicate_matrix_trials_cannot_establish_capacity(self):
        for trials in ([trial(seed=11), trial(seed=22)], [trial(seed=seed) for seed in SEEDS] + [trial()]):
            result = gate.build_gate(trials, POLICY, ["aerostore"], [32], [1], SEEDS)
            self.assertIsNone(result["capacity_bounds"]["aerostore"]["best_tested_qualified_rate"])

    def test_working_source_fingerprint_detects_dirty_and_new_proof_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "aerostore_core/src").mkdir(parents=True)
            source = root / "aerostore_core/src/lib.rs"
            source.write_text("before")
            before = gate.snapshot_sources(root)
            source.write_text("after")
            self.assertNotEqual(before, gate.snapshot_sources(root))
            source.write_text("before")
            self.assertEqual(before, gate.snapshot_sources(root))
            (root / "verification").mkdir()
            (root / "verification/new.lean").write_text("theorem")
            self.assertNotEqual(before, gate.snapshot_sources(root))

    def test_timeout_kills_descendant_and_redacts_logs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pid_file = root / "child.pid"
            code = "import os,subprocess,time,pathlib; p=subprocess.Popen(['sleep','30'], start_new_session=True); pathlib.Path(%r).write_text(str(p.pid)); print('SECRET_URL',flush=True); time.sleep(30)" % str(pid_file)
            outcome = gate.run_process([sys.executable, "-c", code], root / "run.log", .2, ("SECRET_URL",))
            self.assertTrue(outcome["timed_out"])
            self.assertLess(outcome["wall_seconds"], 5)
            self.assertNotIn("SECRET_URL", (root / "run.log").read_text())
            pid = int(pid_file.read_text())
            for _ in range(100):
                path = Path(f"/proc/{pid}/stat")
                if not path.exists() or path.read_text().rsplit(") ", 1)[1].startswith("Z"):
                    break
                time.sleep(.01)
            else:
                self.fail("descendant survived timeout cleanup")

    def test_timeout_removes_only_exact_private_configs_inside_owned_trial(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            owned = root / "trial"
            owned.mkdir()
            (owned / ".qualification-owned-trial").write_text("token")
            external = root / "external"
            external.mkdir()
            (external / "private-config.json").write_text("outside-owned-tree")
            (owned / "external-link").symlink_to(external, target_is_directory=True)
            child_code = ("import pathlib,time; p=pathlib.Path(%r); (p/'case').mkdir(); "
                          "(p/'case/private-config.json').write_text('CREDENTIAL'); "
                          "(p/'case/worker-0.json').write_text('CREDENTIAL'); "
                          "(p/'case/worker-not-number.json').write_text('retain'); "
                          "(p/'case/history.jsonl').write_text('retain'); time.sleep(30)") % str(owned)
            outcome = gate.run_process([sys.executable, "-c", child_code], owned / "run.log", .2,
                                       ("CREDENTIAL",), owned, "token")
            self.assertTrue(outcome["timed_out"])
            self.assertTrue(outcome["owned_processes_terminated"])
            self.assertEqual(outcome["private_configs_removed"], ["case/private-config.json", "case/worker-0.json"])
            self.assertFalse((owned / "case/private-config.json").exists())
            self.assertFalse((owned / "case/worker-0.json").exists())
            self.assertEqual((external / "private-config.json").read_text(), "outside-owned-tree")
            self.assertTrue((owned / "case/worker-not-number.json").exists())
            self.assertTrue((owned / "case/history.jsonl").exists())
            with self.assertRaises((ValueError, FileNotFoundError)):
                gate.cleanup_private_configs(root, "token")

    def test_missing_binary_invalidates_stale_success_without_running_trials(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "campaign.json").write_text(json.dumps({"passed": True}))
            code = gate.main(["--binary", str(root / "missing"), "--output", str(root), "--engines", "aerostore", "--slo-ms", "100"])
            self.assertEqual(code, 1)
            report = json.loads((root / "campaign.json").read_text())
            self.assertFalse(report["passed"])
            self.assertFalse(report["completed"])
            self.assertEqual(report["trials"], [])

    def test_invalid_arguments_also_invalidate_stale_success(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "campaign.json").write_text(json.dumps({"passed": True}))
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                gate.main(["--binary", str(root / "missing"), "--output=" + str(root),
                           "--workers", "0", "--slo-ms", "100"])
            report = json.loads((root / "campaign.json").read_text())
            self.assertFalse(report["passed"])
            self.assertFalse(report["completed"])
            self.assertEqual(report["stage"], "configuration")


class RollingQualificationTests(unittest.TestCase):
    def test_rolling_configuration_bounds_and_legacy_defaults(self):
        self.assertEqual(gate.rolling_config({}), (0, 0))
        for cycle, retention in ((16, 1), (1000000, 3600)):
            self.assertEqual(gate.rolling_config({"rolling_cycle_messages": cycle,
                                                  "rolling_retention_seconds": retention,
                                                  "maintenance_mode": "sweep"}), (cycle, retention))
        for cycle, retention in ((0, 1), (16, 0), (15, 1), (1000001, 1),
                                  (16, 3601), (-1, 0), (True, 1), (16, True),
                                  (16.0, 1), (16, None)):
            with self.subTest(cycle=cycle, retention=retention), self.assertRaises(ValueError):
                gate.rolling_config({"rolling_cycle_messages": cycle,
                                     "rolling_retention_seconds": retention})
        with self.assertRaises(ValueError):
            gate.rolling_config({"rolling_cycle_messages": 16, "rolling_retention_seconds": 1})

    def test_rolling_companion_keys_include_both_parameters(self):
        config = calibrated_trial()["config"]
        self.assertEqual(gate.key(config), gate.key({**config, **gate.ROLLING_DEFAULTS}))
        rolling = {**config, "rolling_cycle_messages": 16, "rolling_retention_seconds": 1, "maintenance_mode": "sweep"}
        for changed in ({}, {"rolling_cycle_messages": 17}, {"rolling_retention_seconds": 2}):
            other = config if not changed else {**rolling, **changed}
            self.assertNotEqual(gate.key(rolling), gate.key(other))

    def test_invalid_rolling_cli_is_rejected_before_execution(self):
        for flags in (["--rolling-cycle-messages", "16"],
                      ["--rolling-retention-seconds", "1"],
                      ["--rolling-cycle-messages", "15", "--rolling-retention-seconds", "1"],
                      ["--rolling-cycle-messages", "1000001", "--rolling-retention-seconds", "1"],
                      ["--rolling-cycle-messages", "16", "--rolling-retention-seconds", "3601"],
                      ["--workload", "fleet", "--rolling-cycle-messages", "16", "--rolling-retention-seconds", "1"]):
            with self.subTest(flags=flags), tempfile.TemporaryDirectory() as directory:
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as caught:
                    gate.main(["--binary", "/nonexistent/benchmark", "--output", directory,
                               "--workload", "calibrated", "--families", "16", "--hot-percent", "0",
                               "--slo-ms", "50", *flags])
                self.assertEqual(caught.exception.code, 2)

    def test_rolling_kind_and_phase_counts_match_independent_enumeration(self):
        for seconds, cycle in ((1, 16), (7, 16), (8, 16), (9, 16), (40, 17), (41, 23)):
            with self.subTest(seconds=seconds, cycle=cycle):
                item = rolling_trial(seconds=seconds, cycle=cycle)
                run, config = item["report"]["runs"][0], item["config"]
                phases = {phase: count for phase, count in gate.rolling_phase_counts(config).items() if count}
                self.assertEqual(phases, run["rolling_lifecycle"]["phase_counts"])
                self.assertEqual(gate.calibrated_corpus(config)["kinds"], run["message_kinds"])
                verdict = gate.assess_trial(item, POLICY)
                self.assertTrue(verdict["execution_valid"], verdict["reasons"])

    def test_rolling_creation_keeps_both_aliases_across_later_generations(self):
        # At ordinals16 and32 mixed signatures would normally be tail-only and
        # callsign-only. Creation overrides both, producing distinct new keys.
        config = {"families": 4, "workers": 4, "arrival_rate": 3, "seconds": 36,
                  "dispatch": "signature-affinity", "affinity_ttl_ms": 100000,
                  "signature_pattern": "mixed", "maintenance_mode": "sweep",
                  "rolling_cycle_messages": 16, "rolling_retention_seconds": 1}
        audit = gate.calibrated_dispatch(config)
        # Golden values were calculated from explicit per-ordinal owner rows,
        # without using the gate's signature/router reconstruction.
        self.assertEqual(audit["worker_counts"], [24, 20, 18, 46])
        self.assertEqual(audit["assignment_fingerprint"], "c15e85f5a3f12a65")
        self.assertEqual(audit["new_signature_misses"], 19)
        self.assertEqual(audit["unique_signatures"], 19)
        self.assertEqual(audit["hits"], 89)
        self.assertEqual(audit["planned_flight_worker_changes"], 45)

    def test_rolling_useful_counts_exclude_all_retirement_controls(self):
        item = rolling_trial()
        run = item["report"]["runs"][0]
        verdict = gate.assess_trial(item, POLICY)
        self.assertTrue(verdict["execution_valid"], verdict["reasons"])
        self.assertTrue(verdict["rolling_coverage_passed"])
        self.assertTrue(verdict["rolling_diagnostic_performance_passed"])
        self.assertTrue(verdict["population_turnover_tested"])
        self.assertEqual(verdict["retirement_control_messages"], 15)
        self.assertEqual(verdict["useful_foreground_messages"], 225)
        self.assertAlmostEqual(verdict["useful_foreground_throughput_with_drain"], 225 / run["elapsed_seconds_including_drain"])
        self.assertLess(verdict["useful_foreground_throughput_with_drain"], verdict["foreground_throughput_with_drain"])
        self.assertIn("retirement", verdict["foreground_p99_scope"])
        for flag in ("performance_passed", "qualified_capacity_trial", "capacity_failure",
                     "calibrated_capacity_qualification_complete", "representative_cadence_coverage_passed"):
            self.assertFalse(verdict[flag], flag)

    def test_rolling_short_runs_and_horizon_endpoint_do_not_claim_recurrence(self):
        for seconds in (1, 10, 31, 32):
            verdict = gate.assess_trial(rolling_trial(seconds=seconds), POLICY)
            self.assertTrue(verdict["execution_valid"], verdict["reasons"])
            self.assertFalse(verdict["rolling_coverage_passed"])
            self.assertFalse(verdict["rolling_diagnostic_performance_passed"])
        verdict = gate.assess_trial(rolling_trial(seconds=33), POLICY)
        self.assertEqual(verdict["rolling_late_maintenance"]["projection"]["completed_jobs"], 2)
        self.assertTrue(verdict["rolling_coverage_passed"])

    def test_reschedule_only_projection_does_not_count_as_useful_late_work(self):
        item = rolling_trial()
        run = item["report"]["runs"][0]
        for job in run["maintenance_jobs"]:
            if job["class"] == "projection" and job["scheduled_ns"] - run["admission_started_ns"] > 30000000000:
                job["outcomes"]["outputs"] = 0
        run["per_kind"]["global_projection"]["outcomes"]["outputs"] = sum(
            job["outcomes"]["outputs"] for job in run["maintenance_jobs"] if job["class"] == "projection")
        verdict = gate.assess_trial(item, POLICY)
        self.assertTrue(verdict["execution_valid"], verdict["reasons"])
        self.assertTrue(verdict["repeated_positive_maintenance_jobs"])
        self.assertEqual(verdict["rolling_late_maintenance"]["projection"]["positive_effect_jobs"], 0)
        self.assertFalse(verdict["rolling_coverage_passed"])

    def test_rolling_actual_reuse_is_required_separately_from_creation_and_retirement(self):
        item = rolling_trial(seconds=40, cycle=64)
        verdict = gate.assess_trial(item, POLICY)
        self.assertTrue(verdict["execution_valid"], verdict["reasons"])
        self.assertEqual(verdict["rolling_lifecycle"]["reused_family_generations"], 0)
        self.assertFalse(verdict["rolling_coverage_passed"])

    def test_rolling_audit_and_config_tampering_fails_closed(self):
        mutations = [
            lambda run: run.pop("rolling_lifecycle"),
            lambda run: run["rolling_lifecycle"].update(passed=False),
            lambda run: run["rolling_lifecycle"]["phase_counts"].update(creation=0),
            lambda run: run["rolling_lifecycle"]["phase_positive_effects"].update(creation=True),
            lambda run: run["rolling_lifecycle"]["phase_outcomes"]["arrival"].update(outputs=999),
            lambda run: run["rolling_lifecycle"].update(created_generations=999),
            lambda run: run["rolling_lifecycle"].update(retired_families=0),
            lambda run: run["rolling_lifecycle"].update(reused_family_generations=999),
            lambda run: run["rolling_lifecycle"].update(physical_families_reused=0),
            lambda run: run.update(population_turnover_tested=False),
            lambda run: run["initial_fleet"].update(live_families=1),
            lambda run: run["final_fleet"].update(live_families=4),
            lambda run: run["calibrated_schedule"].update(housekeeping_seed_cohorts=3),
            lambda run: run["calibrated_schedule"]["rolling_policy"].update(capacity_qualified=True),
            lambda run: run["calibrated_schedule"]["rolling_policy"].update(terminal_retention_seconds=1),
            lambda run: run["calibrated_schedule"]["rolling_policy"].update(message_id_stride=8),
            lambda run: run["calibrated_schedule"]["rolling_policy"].pop("message_id"),
            lambda run: run["calibrated_schedule"].pop("rolling_cycle_messages"),
        ]
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                item = rolling_trial()
                mutate(item["report"]["runs"][0])
                self.assertFalse(gate.assess_trial(item, POLICY)["execution_valid"])

    def test_rolling_stale_or_duplicate_effects_cannot_earn_useful_rate(self):
        for field in ("ignored_stale", "duplicate_messages", "allocation_deferred", "missing_family"):
            with self.subTest(field=field):
                item = rolling_trial()
                run = item["report"]["runs"][0]
                run["rolling_lifecycle"]["phase_outcomes"]["position"][field] = 1
                run["per_kind"]["position"]["outcomes"][field] = 1
                verdict = gate.assess_trial(item, POLICY)
                self.assertTrue(verdict["execution_valid"], verdict["reasons"])
                self.assertTrue(verdict["history_verified"])
                self.assertFalse(verdict["useful_work_passed"])
                self.assertFalse(verdict["rolling_coverage_passed"])
                self.assertIsNone(verdict["useful_foreground_messages"])
                self.assertIsNone(verdict["useful_foreground_throughput_with_drain"])

    def test_rolling_metrics_need_exact_companion_and_never_gain_own_history(self):
        item = rolling_trial(evidence="metrics")
        without = gate.assess_trial(item, POLICY)
        self.assertTrue(without["rolling_coverage_passed"])
        self.assertFalse(without["correctness_companion_verified"])
        own_key = gate.key(item["config"])
        paired = gate.assess_trial(item, POLICY, {own_key})
        self.assertTrue(paired["correctness_companion_verified"])
        self.assertFalse(paired["history_verified"])
        for field in gate.ROLLING_DEFAULTS:
            wrong = {**item["config"], field: item["config"][field] + 1}
            self.assertFalse(gate.assess_trial(item, POLICY, {gate.key(wrong)})["correctness_companion_verified"])
        for flag in ("qualified_capacity_trial", "capacity_failure", "calibrated_capacity_qualification_complete"):
            self.assertFalse(paired[flag])

    def test_explicit_zero_controls_cannot_omit_rolling_report_metadata(self):
        old = calibrated_trial()
        self.assertTrue(gate.assess_trial(old, POLICY)["execution_valid"])
        for config in (old["config"], old["report"]["config"]):
            config.update(gate.ROLLING_DEFAULTS)
        self.assertFalse(gate.assess_trial(old, POLICY)["execution_valid"])
        old["report"]["runs"][0]["calibrated_schedule"].update(gate.ROLLING_DEFAULTS)
        self.assertTrue(gate.assess_trial(old, POLICY)["execution_valid"])

    def test_rolling_and_control_corpora_cannot_mix_in_one_matrix(self):
        result = gate.build_gate([rolling_trial(), sweep_trial(seconds=40)], POLICY,
                                 ["aerostore"], [6], [2], [11])
        self.assertFalse(result["corpus_configuration_matches"])
        self.assertTrue(all(not item["assessment"]["execution_valid"] for item in result["trial_assessments"]))


class RetryDiagnosticGateTests(unittest.TestCase):
    @staticmethod
    def trace(count=3, enabled=True):
        samples = []
        for attempt in range(max(0, count - 32), count) if enabled else []:
            samples.append(dict(message_id=9, attempt_index=attempt,
                started_ns=attempt*10+1, finished_ns=attempt*10+2, cleanup_finished_ns=attempt*10+3,
                error_kind="conflict", error="transaction conflict", cleanup_ok=True, cleanup_error=None,
                retry_causes_delta={"commit:serialization_failure":1},
                diagnostics_delta={"conflict_origin:commit:captured_predicate_stamp:event_time":1},
                cleanup_retry_causes_delta={}, cleanup_diagnostics_delta={}, counter_regression=False,
                metrics_status={"source":"local_adapter","complete":True}))
        return dict(version=1, enabled=enabled, sample_limit=32, failed_attempts=count,
                    dropped_attempts=count-len(samples), samples=samples)

    def test_exact_tail_and_disabled_counts_retain_terminal_attempt(self):
        for enabled in (False, True):
            trace = self.trace(129, enabled)
            self.assertEqual(gate.retry_trace_errors(trace, enabled), [])
            if enabled:
                self.assertEqual(trace["samples"][-1]["attempt_index"], 128)
                self.assertEqual(trace["dropped_attempts"], 97)
            for field, value in (("failed_attempts", True), ("dropped_attempts", 0), ("sample_limit", 64)):
                changed = copy.deepcopy(trace); changed[field] = value
                self.assertTrue(gate.retry_trace_errors(changed, enabled), field)

    def test_stale_remote_counters_must_remain_explicitly_incomplete(self):
        trace = self.trace(1)
        status = dict(source="service_cache", complete=False, last_attempted_sequence=8,
                      last_completed_sequence=7, last_metrics_sequence=7, transport_failed=True, connected=False)
        trace["samples"][0]["metrics_status"] = status
        self.assertEqual(gate.retry_trace_errors(trace, True), [])
        status["complete"] = True
        self.assertIn("stale service counters reported complete", gate.retry_trace_errors(trace, True))
        status.update(last_attempted_sequence=7, transport_failed=False)
        self.assertEqual(gate.retry_trace_errors(trace, True), [])

    def test_attempt_order_cleanup_and_counter_negative_controls(self):
        changes = [("attempt_index", 9), ("started_ns", 0), ("cleanup_ok", False),
                   ("counter_regression", True), ("diagnostics_delta", {"x":-1}),
                   ("error", "x"*513), ("metrics_status", {"source":"local_adapter","complete":False})]
        for field, value in changes:
            trace = self.trace(); trace["samples"][1][field] = value
            self.assertTrue(gate.retry_trace_errors(trace, True), field)
        trace = self.trace(); trace["samples"][1]["message_id"] = 10
        self.assertTrue(gate.retry_trace_errors(trace, True))

    def test_companions_match_diagnostic_mode_and_native_expiry_policy(self):
        legacy = trial(evidence="metrics")
        explicit = {**legacy["config"], **gate.EXPERIMENT_DEFAULTS}
        self.assertEqual(gate.key(legacy["config"]), gate.key(explicit))
        for changed in ({"expiry_index_policy":"housekeeping"}, {"retry_diagnostics":True},
                        {"due_index_policy":"ordered"}, {"due_index_origin":-100},
                        {"due_index_width":7}, {"expiry_publication_policy":"ordered"},
                        {"expiry_index_origin":-100}, {"expiry_index_width":7}):
            self.assertNotEqual(gate.key(explicit), gate.key({**explicit, **changed}))
            self.assertFalse(gate.assess_trial(legacy, POLICY, {gate.key({**explicit, **changed})})["correctness_companion_verified"])

    def test_due_publication_report_requires_exact_requested_and_effective_parameters(self):
        self.assertEqual(gate.experiment_report_errors({}, {"engine":"aerostore"}), [])
        # A modern explicitly configured hashed control cannot erase all its
        # publication metadata and masquerade as a historical default report.
        self.assertTrue(gate.experiment_report_errors({}, {"engine":"aerostore", **gate.DUE_INDEX_DEFAULTS}))
        for engine in ("aerostore", "postgres"):
            config = {"engine":engine, "due_index_policy":"ordered", "due_index_origin":-100,
                      "due_index_width":7}
            run = {**config, "effective_due_index_policy":"postgres" if engine=="postgres" else "ordered",
                   "effective_due_index_origin":None if engine=="postgres" else -100,
                   "effective_due_index_width":None if engine=="postgres" else 7}
            self.assertEqual(gate.experiment_report_errors(run, config), [])
            for field, value in (("due_index_policy","hashed"), ("due_index_origin",-99),
                                 ("due_index_width",8), ("effective_due_index_policy","hashed"),
                                 ("effective_due_index_origin",101), ("effective_due_index_width",1)):
                with self.subTest(engine=engine, field=field):
                    self.assertTrue(gate.experiment_report_errors({**run, field:value}, config))
                    missing = copy.deepcopy(run); missing.pop(field)
                    # None is a deliberate inactive-parameter representation;
                    # missing active ordered parameters cannot select defaults.
                    if not (engine=="postgres" and field in ("effective_due_index_origin", "effective_due_index_width")):
                        self.assertTrue(gate.experiment_report_errors(missing, config))
        for field, value in (("due_index_policy","unknown"), ("due_index_width",0),
                             ("due_index_width",2**64), ("due_index_width",True),
                             ("due_index_origin",2**63), ("due_index_origin",-(2**63)-1)):
            self.assertTrue(gate.experiment_report_errors({}, {field:value}))

    def test_invalid_due_index_parameters_fail_before_binary_resolution(self):
        for flags in (["--due-index-width","0"], ["--due-index-width",str(2**64)],
                      ["--due-index-origin",str(2**63)], ["--due-index-origin",str(-(2**63)-1)]):
            with self.subTest(flags=flags), tempfile.TemporaryDirectory() as directory:
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                    gate.main(["--binary","/missing/benchmark","--output",directory,
                               "--engines","aerostore","--slo-ms","100",*flags])
                self.assertEqual(error.exception.code, 2)

    def test_expiry_publication_requested_and_effective_metadata_are_exact(self):
        for engine in ("aerostore", "service-unix", "service-tcp", "postgres"):
            for publication in ("hashed", "ordered"):
                config = dict(engine=engine, expiry_publication_policy=publication,
                              expiry_index_origin=-100, expiry_index_width=7)
                effective = "postgres" if engine == "postgres" else publication
                run = {**config, "effective_expiry_publication_policy":effective,
                       "effective_expiry_index_origin":-100 if effective == "ordered" else None,
                       "effective_expiry_index_width":7 if effective == "ordered" else None}
                self.assertEqual(gate.experiment_report_errors(run, config), [])
                for field in (*gate.EXPIRY_PUBLICATION_DEFAULTS, "effective_expiry_publication_policy",
                              "effective_expiry_index_origin", "effective_expiry_index_width"):
                    with self.subTest(engine=engine, publication=publication, field=field):
                        missing = copy.deepcopy(run); missing.pop(field)
                        self.assertTrue(gate.experiment_report_errors(missing, config))
                        self.assertTrue(gate.experiment_report_errors({**run, field:True}, config))
                for field, value in (("expiry_publication_policy","unknown"),
                                     ("expiry_index_origin",-99), ("expiry_index_width",8),
                                     ("effective_expiry_publication_policy","unknown"),
                                     ("effective_expiry_index_origin",101), ("effective_expiry_index_width",1)):
                    self.assertTrue(gate.experiment_report_errors({**run, field:value}, config), (engine,publication,field))

    def test_expiry_publication_legacy_defaults_and_erased_modern_metadata(self):
        legacy = trial()
        self.assertTrue(gate.assess_trial(legacy, POLICY)["execution_valid"])
        self.assertEqual(gate.experiment_report_errors({}, {"engine":"postgres"}), [])
        modern = copy.deepcopy(legacy)
        modern["config"].update(gate.EXPIRY_PUBLICATION_DEFAULTS)
        modern["report"]["config"].update(gate.EXPIRY_PUBLICATION_DEFAULTS)
        # Explicit current configuration cannot masquerade as historical output.
        self.assertFalse(gate.assess_trial(modern, POLICY)["execution_valid"])
        run = modern["report"]["runs"][0]
        run.update(**gate.EXPIRY_PUBLICATION_DEFAULTS, effective_expiry_publication_policy="hashed",
                   effective_expiry_index_origin=None, effective_expiry_index_width=None)
        self.assertTrue(gate.assess_trial(modern, POLICY)["execution_valid"])
        # Effective-only metadata is also a modern report, even with legacy input.
        for field in ("effective_expiry_publication_policy", "effective_expiry_index_origin",
                      "effective_expiry_index_width"):
            self.assertTrue(gate.experiment_report_errors({field:run[field]}, legacy["config"]))

    def test_expiry_publication_config_numeric_bounds_and_types(self):
        for origin, width in ((-(2**63),1), (2**63-1,2**64-1)):
            config = dict(engine="aerostore", expiry_publication_policy="ordered",
                          expiry_index_origin=origin, expiry_index_width=width)
            run = {**config, "effective_expiry_publication_policy":"ordered",
                   "effective_expiry_index_origin":origin, "effective_expiry_index_width":width}
            self.assertEqual(gate.experiment_report_errors(run, config), [])
        for field, value in (("expiry_publication_policy","unknown"), ("expiry_publication_policy",[]),
                             ("expiry_index_origin",True), ("expiry_index_origin",1.0),
                             ("expiry_index_origin",None), ("expiry_index_origin",2**63),
                             ("expiry_index_origin",-(2**63)-1), ("expiry_index_width",True),
                             ("expiry_index_width",0), ("expiry_index_width",-1),
                             ("expiry_index_width",2**64), ("expiry_index_width","7")):
            with self.subTest(field=field, value=value):
                self.assertIn("invalid expiry-index publication configuration",
                              gate.experiment_report_errors({}, {field:value}))

    def test_expiry_publication_companions_and_report_config_tamper(self):
        item = trial(evidence="metrics")
        config = dict(expiry_publication_policy="ordered", expiry_index_origin=-100, expiry_index_width=7)
        item["config"].update(config); item["report"]["config"].update(config)
        item["report"]["runs"][0].update(**config, effective_expiry_publication_policy="ordered",
                                        effective_expiry_index_origin=-100, effective_expiry_index_width=7)
        self.assertTrue(gate.assess_trial(item, POLICY, {gate.key(item["config"])})["correctness_companion_verified"])
        self.assertFalse(gate.assess_trial(item, POLICY)["history_verified"])
        for field, value in (("expiry_publication_policy","hashed"), ("expiry_index_origin",-99),
                             ("expiry_index_width",8)):
            other = {**item["config"], field:value}
            self.assertFalse(gate.assess_trial(item, POLICY, {gate.key(other)})["correctness_companion_verified"])
            altered = copy.deepcopy(item); altered["report"]["config"][field] = value
            self.assertFalse(gate.assess_trial(altered, POLICY, {gate.key(item["config"])})["execution_valid"])
            altered = copy.deepcopy(item); altered["report"]["config"].pop(field)
            self.assertFalse(gate.assess_trial(altered, POLICY, {gate.key(item["config"])})["execution_valid"])

    def test_invalid_expiry_publication_cli_rejects_before_binary_resolution(self):
        for flags in (["--expiry-publication","unknown"], ["--expiry-index-width","0"],
                      ["--expiry-index-width","-1"], ["--expiry-index-width",str(2**64)],
                      ["--expiry-index-origin",str(2**63)], ["--expiry-index-origin",str(-(2**63)-1)]):
            with self.subTest(flags=flags), tempfile.TemporaryDirectory() as directory:
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                    gate.main(["--binary","/missing/benchmark","--output",directory,
                               "--engines","aerostore","--slo-ms","100",*flags])
                self.assertEqual(error.exception.code, 2)

    def test_expiry_publication_cli_reaches_config_and_command_without_changing_other_selectors(self):
        for values in (gate.EXPIRY_PUBLICATION_DEFAULTS,
                       dict(expiry_publication_policy="ordered", expiry_index_origin=-(2**63), expiry_index_width=1),
                       dict(expiry_publication_policy="ordered", expiry_index_origin=2**63-1, expiry_index_width=2**64-1)):
            with self.subTest(values=values), tempfile.TemporaryDirectory() as directory:
                binary = Path(directory) / "benchmark"; binary.write_bytes(b"never executed")
                output = Path(directory) / "evidence"
                flags = [] if values == gate.EXPIRY_PUBLICATION_DEFAULTS else [
                    "--expiry-publication",values["expiry_publication_policy"],
                    "--expiry-index-origin",str(values["expiry_index_origin"]),
                    "--expiry-index-width",str(values["expiry_index_width"])]
                with patch.object(gate, "snapshot_sources", return_value={"sha256":"source","files":{}}), \
                     patch.object(gate, "host_info", return_value={}), \
                     patch.object(gate.subprocess, "check_output", return_value="fixture metadata"), \
                     patch.object(gate, "run_process", side_effect=RuntimeError("stop before execution")) as start:
                    with contextlib.redirect_stdout(io.StringIO()):
                        self.assertEqual(gate.main(["--binary",str(binary),"--output",str(output),
                            "--engines","aerostore","--rates","32","--workers","1","--seeds","11",
                            "--slo-ms","100",*flags]), 1)
                    start.assert_called_once()
                cell = json.loads((output / "campaign.json").read_text())["trials"][0]
                self.assertEqual({field:cell["config"][field] for field in values}, values)
                for flag, value in (("--expiry-publication",values["expiry_publication_policy"]),
                                    ("--expiry-index-origin",str(values["expiry_index_origin"])),
                                    ("--expiry-index-width",str(values["expiry_index_width"])),
                                    ("--expiry-index","all-active"), ("--due-index","hashed")):
                    self.assertEqual(cell["command"][cell["command"].index(flag)+1], value)

    def test_successful_report_reconciles_failures_and_feature_support(self):
        item = trial(); item["config"].update(expiry_index_policy="housekeeping", retry_diagnostics=True)
        item["report"]["config"].update(item["config"])
        run = item["report"]["runs"][0]
        run.update(expiry_index_policy="housekeeping", effective_expiry_index_policy="housekeeping",
                   retry_diagnostics_compiled=True, worker_retry_diagnostics=[self.trace()], retries=3)
        self.assertTrue(gate.assess_trial(item, POLICY)["execution_valid"])
        for field, value in (("retries",2), ("retry_diagnostics_compiled",False),
                             ("effective_expiry_index_policy","all-active"), ("worker_retry_diagnostics",[])):
            bad = copy.deepcopy(item); bad["report"]["runs"][0][field] = value
            self.assertFalse(gate.assess_trial(bad, POLICY)["execution_valid"], field)

        for changes in ({"error_kind":"fatal"}, {"cleanup_ok":False,"cleanup_error":"failed abort"},
                        {"attempt_index":128},
                        {"metrics_status":dict(source="service_cache", complete=False,
                         last_attempted_sequence=8,last_completed_sequence=7,last_metrics_sequence=7,
                         transport_failed=True,connected=False)}):
            bad = copy.deepcopy(item)
            bad["report"]["runs"][0]["worker_retry_diagnostics"][0]["samples"][-1].update(changes)
            self.assertFalse(gate.assess_trial(bad, POLICY)["execution_valid"], changes)


if __name__ == "__main__":
    unittest.main()
