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


class QualificationTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
