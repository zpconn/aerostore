#!/usr/bin/env python3
"""Screen-policy and paired-comparison tests; no benchmark processes."""
import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import hyperfeed_screen_assessment as screen
import test_hyperfeed_qualification as fixtures
import test_hyperfeed_capacity as capacity_fixtures


def evidence(lane="maintenance"):
    data = capacity_fixtures.evidence()
    trial = data["trial"]
    if lane in {"foreground", "burst"}:
        trial = fixtures.calibrated_trial(seconds=120 if lane == "burst" else 30, rate=6, workers=2, families=4,
            projection=300, housekeeping=300, evidence="metrics")
        config, run = trial["config"], trial["report"]["runs"][0]
        config.update(maintenance_mode="sweep", projection_batch_size=4,
                      housekeeping_batch_size=32, max_maintenance_batches=4096)
        trial["report"]["config"].update(config)
        run["calibrated_schedule"].update(maintenance_mode="sweep", max_maintenance_batches=4096,
            maintenance_scope="complete_sweep_batched_transactions", global_maintenance_sweep_complete=False)
        run["maintenance_jobs"] = []
        run["maintenance_job_audit"] = dict(checked=True, passed=True, completed_jobs=0,
            committed_batches=0, nonempty_batches=0, terminal_batches=0, processed_rows=0,
            scope="complete_sweep_batched_transactions")
        run["transaction_kinds"] = dict(run["message_kinds"])
        run["completed_transactions"] = run["completed_messages"]
        for value in run["per_kind"].values():
            value["transactions"] = value["completed"]
        old = data["trial"]["report"]["runs"][0]
        for field in ("retention_samples", "after_drain", "native_audit"):
            run[field] = copy.deepcopy(old[field])
        run["capacity_accounting"] = capacity_fixtures.accounting(trial)
    trial["source_before_sha256"] = trial["source_after_sha256"] = "a" * 64
    trial["binary_before_sha256"] = trial["binary_after_sha256"] = "b" * 64
    envelope = {"result": data["envelope"], "readiness": data["readiness"], "samples": data["memory_samples"]}
    policy = lane if lane in {"foreground", "burst"} else {
        "lane": "maintenance", "seconds": 11, "warmup_seconds": 1,
        "projection_interval_seconds": 1, "housekeeping_interval_seconds": 2}
    return trial, envelope, policy


def assessed(lane="foreground", p99=10, variant="baseline", seed=29, same_binary=False):
    trial, envelope, policy = evidence(lane)
    trial["config"]["seed"] = trial["report"]["config"]["seed"] = seed
    if variant == "candidate" and not same_binary:
        trial["binary_before_sha256"] = trial["binary_after_sha256"] = "c" * 64
    run = trial["report"]["runs"][0]
    run["workload_classes"]["foreground"]["p99_us_including_retries"] = p99 * 1000
    run["capacity_accounting"]["foreground"]["p99_us_including_retries"] = p99 * 1000
    result = screen.assess_screen_trial(trial, envelope, policy)
    assert result["valid_measurement"], result
    return {"variant": variant, "lane": lane, "seed": seed, "assessment": result}


def pairs(base=10, candidate=10, same_binary=False):
    return [assessed(p99=p99, variant=variant, seed=seed, same_binary=same_binary)
            for seed, variant, p99 in ((29, "baseline", base), (29, "candidate", candidate),
                                       (30, "candidate", candidate), (30, "baseline", base))]


class ScreenTests(unittest.TestCase):
    def test_preset_policies_are_fresh_and_do_not_mutate_sustained_policy(self):
        before = copy.deepcopy(screen.capacity.DEFAULT_POLICY)
        self.assertEqual(screen.lane_policy("foreground")["seconds"], 30)
        self.assertEqual(screen.lane_policy("maintenance")["seconds"], 40)
        changed = screen.lane_policy("foreground")
        changed["seconds"] = 99
        self.assertEqual(screen.lane_policy("foreground")["seconds"], 30)
        self.assertEqual(before, screen.capacity.DEFAULT_POLICY)

    def test_foreground_intentionally_has_no_maintenance(self):
        result = screen.assess_screen_trial(*evidence("foreground"))
        self.assertEqual(result["classification"], "screen_passed", result)
        self.assertTrue(result["screen_requirements_met"])
        self.assertEqual(result["metrics"]["maintenance"]["projection"]["offered"], 0)
        self.assertFalse(result["sustained_capacity_established"])

    def test_burst_only_extends_foreground_duration_and_remains_a_screen(self):
        foreground, burst = screen.lane_policy("foreground"), screen.lane_policy("burst")
        self.assertEqual({key for key in foreground if foreground[key] != burst[key]},
                         {"lane", "seconds"})
        self.assertEqual(burst["seconds"], 120)
        self.assertEqual(burst["warmup_seconds"], 5)
        self.assertEqual(burst["foreground_p99_ms"], 50)
        result = screen.assess_screen_trial(*evidence("burst"))
        self.assertEqual(result["classification"], "screen_passed", result)
        self.assertEqual(result["metrics"]["completed_messages_per_second"], 6)
        self.assertEqual(result["metrics"]["foreground_completed_including_drain"], 720)
        self.assertEqual(result["metrics"]["maintenance"]["projection"]["offered"], 0)
        self.assertEqual(result["maintenance_scope"], "not_observed_first_tick_after_screen")
        self.assertTrue(result["screening_only"])
        self.assertFalse(result["sustained_capacity_established"])

    def test_burst_rejects_wrong_duration_and_accelerated_timers(self):
        trial, envelope, _ = evidence("foreground")
        result = screen.assess_screen_trial(trial, envelope, "burst")
        self.assertEqual(result["classification"], "invalid_evidence")
        self.assertIn("configuration differs", result["reasons"][0])
        for field in ("projection_interval_seconds", "housekeeping_interval_seconds"):
            with self.assertRaisesRegex(ValueError, "first maintenance tick"):
                screen.lane_policy({"lane": "burst", field: 30})

    def test_burst_retains_expected_config_binding_and_latency_failure(self):
        trial, envelope, _ = evidence("burst")
        result = screen.assess_screen_trial(trial, envelope,
            {"lane": "burst", "expected_config": {"arrival_rate": 7}})
        self.assertEqual(result["classification"], "invalid_evidence")
        result = assessed(lane="burst", p99=51)["assessment"]
        self.assertEqual(result["classification"], "completed_policy_failure", result)
        self.assertTrue(result["valid_measurement"])
        self.assertFalse(result["screen_requirements_met"])
        self.assertFalse(result["sustained_capacity_established"])

    def test_maintenance_requires_complete_useful_jobs(self):
        result = screen.assess_screen_trial(*evidence())
        self.assertEqual(result["classification"], "screen_passed", result)
        trial, envelope, policy = evidence()
        policy["minimum_positive_maintenance_jobs"] = 100
        result = screen.assess_screen_trial(trial, envelope, policy)
        self.assertEqual(result["classification"], "completed_policy_failure", result)
        self.assertTrue(any("positive" in reason for reason in result["reasons"]))

    def test_foreground_cannot_silently_use_accelerated_timers(self):
        trial, envelope, _ = evidence("foreground")
        result = screen.assess_screen_trial(trial, envelope, {"lane": "foreground", "projection_interval_seconds": 5})
        self.assertEqual(result["classification"], "invalid_evidence")

    def test_p99_includes_retries_and_drain(self):
        result = assessed(p99=51)["assessment"]
        self.assertTrue(result["valid_measurement"])
        self.assertFalse(result["screen_requirements_met"])
        self.assertEqual(result["metrics"]["foreground_p99_ms"], 51)

    def test_drain_cannot_inflate_received_throughput(self):
        trial, envelope, policy = evidence("foreground")
        accounting = trial["report"]["runs"][0]["capacity_accounting"]
        accounting["windows"][-1]["completions"] -= 5
        capacity_fixtures.recalculate_windows(accounting)
        result = screen.assess_screen_trial(trial, envelope, policy)
        self.assertEqual(result["classification"], "completed_policy_failure", result)
        self.assertAlmostEqual(result["metrics"]["completed_messages_per_second"], 175 / 30)
        self.assertEqual(result["metrics"]["foreground_completed_including_drain"], 180)

    def test_fork_count_inflation_is_invalid(self):
        trial, envelope, policy = evidence("foreground")
        trial["report"]["runs"][0]["capacity_accounting"]["foreground"]["offered"] *= 7
        result = screen.assess_screen_trial(trial, envelope, policy)
        self.assertEqual(result["classification"], "invalid_evidence")

    def test_wrong_order_and_structural_errors_cannot_pass(self):
        for field in ("per_flight_order", "invariants"):
            trial, envelope, policy = evidence("foreground")
            trial["report"]["runs"][0][field]["passed"] = False
            result = screen.assess_screen_trial(trial, envelope, policy)
            self.assertFalse(result["valid_measurement"], result)

    def test_retry_exhaustion_generator_and_timeout_stay_distinct(self):
        for error, expected in (("after 128 retries: transaction conflict", "operational_failure"),
                                ("message_cap exceeded", "censored_generator"),
                                ("oracle exceeded budget", "censored_oracle"),
                                ("no worker progress for 60 seconds", "censored_timeout")):
            trial, envelope, policy = evidence("foreground")
            trial.update(exit_code=2, error=error)
            result = screen.assess_screen_trial(trial, envelope, policy)
            self.assertEqual(result["classification"], expected, result)
            self.assertFalse(result["valid_measurement"])

    def test_swap_is_resource_censoring(self):
        trial, envelope, policy = evidence("foreground")
        envelope["result"]["final_accounting"]["memory.swap.peak"] = "1"
        result = screen.assess_screen_trial(trial, envelope, policy)
        self.assertEqual(result["classification"], "censored_resources", result)

    def test_missing_resource_receipts_fail_closed(self):
        trial, _, policy = evidence("foreground")
        result = screen.assess_screen_trial(trial, {}, policy)
        self.assertEqual(result["classification"], "invalid_evidence")

    def test_maintenance_deadline_failure_is_not_a_pass(self):
        trial, envelope, policy = evidence()
        run = trial["report"]["runs"][0]
        job = run["capacity_accounting"]["maintenance"]["projection"]["jobs"][0]
        job["received_ns"] = job["deadline_ns"]
        # A mismatched original receipt must fail closed rather than trusting
        # modified summary timing.
        result = screen.assess_screen_trial(trial, envelope, policy)
        self.assertEqual(result["classification"], "invalid_evidence")

    def test_consistent_late_maintenance_receipts_fail_requirements(self):
        trial, envelope, policy = evidence()
        run = trial["report"]["runs"][0]
        job = run["capacity_accounting"]["maintenance"]["projection"]["jobs"][0]
        job["received_ns"] = job["deadline_ns"]
        original = next(x for x in run["maintenance_jobs"] if x["class"] == "projection" and x["job_ordinal"] == 0)
        original["received_ns"] = job["received_ns"]
        for name in ("projection", "maintenance"):
            selected = [x for x in run["maintenance_jobs"] if name == "maintenance" or x["class"] == name]
            run["workload_classes"][name]["p99_us_including_retries"] = max(x["received_ns"] - x["scheduled_ns"] for x in selected) / 1000
        result = screen.assess_screen_trial(trial, envelope, policy)
        self.assertEqual(result["classification"], "completed_policy_failure", result)
        self.assertTrue(any("next-tick" in reason for reason in result["reasons"]))


class ComparisonTests(unittest.TestCase):
    def test_identical_measurements_are_neutral(self):
        self.assertEqual(screen.compare_screens(pairs())["classification"], "neutral")

    def test_latency_gain_at_same_offered_throughput_is_headroom_only(self):
        result = screen.compare_screens(pairs(candidate=8))
        self.assertEqual(result["classification"], "promising", result)
        self.assertFalse(result["capacity_gain_established"])
        self.assertTrue(result["needs_sustained_qualification"])
        self.assertEqual(result["pairs"][0]["completed_throughput_ratio"], 1)

    def test_same_binary_control_never_picks_winner(self):
        result = screen.compare_screens(pairs(candidate=8, same_binary=True), {"identical_binary": True})
        self.assertEqual(result["decision"], "control_only", result)
        self.assertNotIn(result["classification"], {"promising", "regression"})

    def test_identical_binary_asymmetric_failure_is_not_a_treatment_regression(self):
        rows = pairs(same_binary=True)
        rows[1]["assessment"].update(classification="operational_failure", valid_measurement=False,
                                    reasons=["retry_exhaustion"])
        result = screen.compare_screens(rows, {"identical_binary": True})
        self.assertEqual(result["decision"], "control_only", result)
        self.assertEqual(result["classification"], "inconclusive", result)

    def test_different_binary_cannot_claim_control(self):
        result = screen.compare_screens(pairs(candidate=8), {"identical_binary": True})
        self.assertEqual(result["classification"], "inconclusive", result)

    def test_repeated_latency_regression_is_reported(self):
        self.assertEqual(screen.compare_screens(pairs(candidate=12))["classification"], "regression")

    def test_small_changes_do_not_pick_winner(self):
        self.assertEqual(screen.compare_screens(pairs(candidate=9.5))["classification"], "neutral")

    def test_ten_percent_latency_gain_at_offered_ceiling_remains_neutral(self):
        result = screen.compare_screens(pairs(base=10, candidate=9))
        self.assertEqual(result["classification"], "neutral", result)
        self.assertFalse(result["same_binary_control"])
        self.assertTrue(all(pair["completed_throughput_ratio"] == 1 for pair in result["pairs"]))
        self.assertFalse(result["capacity_gain_established"])

    def test_one_good_pair_does_not_pick_winner(self):
        rows = pairs(candidate=8)
        rows[2] = assessed(p99=11, variant="candidate", seed=30)
        self.assertNotEqual(screen.compare_screens(rows)["classification"], "promising")

    def test_missing_duplicate_or_wrong_seed_is_inconclusive(self):
        for rows in (pairs()[:-1], pairs() + [pairs()[0]], pairs()[:2]):
            self.assertEqual(screen.compare_screens(rows)["classification"], "inconclusive")

    def test_mismatched_configuration_is_inconclusive(self):
        rows = pairs()
        rows[1]["assessment"]["config"]["arrival_rate"] += 1
        self.assertEqual(screen.compare_screens(rows)["classification"], "inconclusive")

    def test_censored_candidate_is_not_a_performance_regression(self):
        rows = pairs()
        rows[1]["assessment"].update(classification="censored_resources", valid_measurement=False)
        self.assertEqual(screen.compare_screens(rows)["classification"], "inconclusive")

    def test_retry_exhaustion_candidate_is_configuration_regression(self):
        rows = pairs()
        rows[1]["assessment"].update(classification="operational_failure", valid_measurement=False,
                                    reasons=["retry_exhaustion"])
        self.assertEqual(screen.compare_screens(rows)["classification"], "regression")

    def test_variant_binary_cannot_change_between_pairs(self):
        rows = pairs(candidate=8)
        rows[2]["assessment"]["binary_sha256"] = "d" * 64
        self.assertEqual(screen.compare_screens(rows)["classification"], "inconclusive")


if __name__ == "__main__":
    unittest.main()
