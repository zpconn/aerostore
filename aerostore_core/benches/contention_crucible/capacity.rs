//! Post-execution accounting, not a capacity acceptance policy. The coordinator
//! has already checked receipt identities against its independent schedule.
use super::{quantile_99, ArrivalPlan, ExecutionSample};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

const SECOND: u64 = 1_000_000_000;

pub struct Plan {
    pub arrivals: ArrivalPlan,
    pub projection_interval_ns: u64,
    pub housekeeping_interval_ns: u64,
}

fn arrivals_before(plan: &ArrivalPlan, cutoff: u64) -> u64 {
    let elapsed = cutoff.saturating_sub(plan.start_ns).min(plan.duration_ns);
    (elapsed as u128 * plan.rate_per_second as u128).div_ceil(SECOND as u128) as u64
}

/// Cheap, explicitly receipt-based observation for existing periodic progress.
/// Includes work executing or in result IPC, not just jobs waiting for a worker.
pub fn progress(plan: &ArrivalPlan, completed: u64, observed_until_ns: u64) -> Value {
    let arrivals = arrivals_before(plan, observed_until_ns);
    json!({"format":"capacity-progress-v1","observed_until_ns":observed_until_ns,
        "foreground_arrivals":arrivals,"foreground_completed_received":completed,
        "foreground_outstanding":arrivals.saturating_sub(completed),
        "scope":"Scheduled foreground arrivals before observation minus successful logical-input receipts already processed; excludes retries, fork updates and maintenance transactions."})
}

pub fn summarize(
    plan: &Plan,
    samples: &[ExecutionSample],
    offered_by_worker: &[u64],
    observed_until_ns: u64,
    execution_completed: bool,
) -> Result<Value, String> {
    let arrivals = &plan.arrivals;
    let end = arrivals
        .start_ns
        .checked_add(arrivals.duration_ns)
        .ok_or("capacity admission clock overflow")?;
    let offered_wide =
        (arrivals.duration_ns as u128 * arrivals.rate_per_second as u128).div_ceil(SECOND as u128);
    if arrivals.duration_ns == 0
        || arrivals.rate_per_second == 0
        || arrivals.workers == 0
        || offered_wide > u64::MAX as u128
        || offered_by_worker.len() != arrivals.workers + 2
        || plan.projection_interval_ns == 0
        || plan.housekeeping_interval_ns == 0
    {
        return Err("invalid capacity accounting dimensions".into());
    }
    let offered = offered_wide as u64;
    if offered_by_worker[..arrivals.workers]
        .iter()
        .map(|n| *n as u128)
        .sum::<u128>()
        != offered_wide
    {
        return Err("capacity foreground offer differs from fixed schedule".into());
    }
    for (worker, period) in [
        (arrivals.workers, plan.projection_interval_ns),
        (arrivals.workers + 1, plan.housekeeping_interval_ns),
    ] {
        if offered_by_worker[worker] != (arrivals.duration_ns - 1) / period {
            return Err("capacity maintenance offer differs from fixed schedule".into());
        }
    }
    let mut observed = vec![0_u64; offered_by_worker.len()];
    let mut previous_finish = vec![0; offered_by_worker.len()];
    let mut identities = BTreeSet::new();
    for sample in samples {
        let role = if sample.worker < arrivals.workers {
            "foreground"
        } else if sample.worker == arrivals.workers {
            "projection"
        } else {
            "housekeeping"
        };
        if sample.worker >= observed.len()
            || sample.class != role
            || !(sample.scheduled_ns <= sample.started_ns
                && sample.started_ns < sample.finished_ns
                && sample.finished_ns <= sample.received_ns
                && sample.received_ns <= observed_until_ns)
            || !identities.insert((sample.class, sample.ordinal))
        {
            return Err("invalid or duplicate capacity execution sample".into());
        }
        let expected = if role == "foreground" {
            arrivals.scheduled_ns(sample.ordinal)
        } else {
            let period = if role == "projection" {
                plan.projection_interval_ns
            } else {
                plan.housekeeping_interval_ns
            };
            sample
                .ordinal
                .checked_add(1)
                .and_then(|n| n.checked_mul(period))
                .filter(|offset| *offset < arrivals.duration_ns)
                .and_then(|offset| arrivals.start_ns.checked_add(offset))
        };
        if expected != Some(sample.scheduled_ns)
            || sample.started_ns < previous_finish[sample.worker]
        {
            return Err(
                "capacity receipt differs from scheduled logical input or worker order".into(),
            );
        }
        previous_finish[sample.worker] = sample.finished_ns;
        observed[sample.worker] += 1;
        if observed[sample.worker] > offered_by_worker[sample.worker] {
            return Err("capacity completion count exceeds worker offer".into());
        }
    }
    if execution_completed && observed != offered_by_worker {
        return Err("completed capacity execution omitted offered jobs".into());
    }
    let mut foreground: Vec<_> = samples.iter().filter(|s| s.class == "foreground").collect();
    foreground.sort_unstable_by_key(|s| s.received_ns);
    let by_sequence: BTreeMap<_, _> = foreground.iter().map(|s| (s.ordinal, *s)).collect();
    let in_admission = foreground.partition_point(|s| s.received_ns < end);
    let fully_observed = observed_until_ns >= end;
    let mut cohorts: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    for sample in &foreground {
        cohorts
            .entry((sample.scheduled_ns - arrivals.start_ns) / SECOND)
            .or_default()
            .push(sample.received_ns - sample.scheduled_ns);
    }
    let mut windows = Vec::new();
    let mut offset = 0;
    let mut oldest_pending = 0;
    while offset < arrivals.duration_ns && arrivals.start_ns + offset < observed_until_ns {
        let end_offset = offset.saturating_add(SECOND).min(arrivals.duration_ns);
        let start = arrivals.start_ns + offset;
        let window_end = arrivals.start_ns + end_offset;
        let cutoff = window_end.min(observed_until_ns);
        let offered_start = arrivals_before(arrivals, start);
        let offered_end = arrivals_before(arrivals, cutoff);
        let received_start = foreground.partition_point(|s| s.received_ns < start);
        let received_end = foreground.partition_point(|s| s.received_ns < cutoff);
        if received_start as u64 > offered_start || received_end as u64 > offered_end {
            return Err("capacity receipts precede their offered arrivals".into());
        }
        while oldest_pending < offered_end
            && by_sequence
                .get(&oldest_pending)
                .is_some_and(|s| s.received_ns < cutoff)
        {
            oldest_pending += 1;
        }
        let oldest_age = (oldest_pending < offered_end).then(|| {
            cutoff
                - arrivals
                    .scheduled_ns(oldest_pending)
                    .expect("admitted sequence")
        });
        let cohort = cohorts.remove(&(offset / SECOND)).unwrap_or_default();
        let planned_arrivals = arrivals_before(arrivals, window_end) - offered_start;
        windows.push(json!({"start_offset_ns":offset,"end_offset_ns":end_offset,
            "observed_end_offset_ns":cutoff-arrivals.start_ns,"fully_observed":cutoff==window_end,
            "planned_arrivals":planned_arrivals,"arrivals":offered_end-offered_start,
            "completions":received_end-received_start,"cumulative_arrivals":offered_end,
            "cumulative_completions":received_end,"backlog_start":offered_start-received_start as u64,
            "backlog_end":offered_end-received_end as u64,"oldest_uncompleted_age_ns":oldest_age,
            "arrival_cohort_completed":cohort.len(),"arrival_cohort_unfinished":planned_arrivals-cohort.len() as u64,
            "arrival_cohort_p99_us_including_retries":quantile_99(cohort),
            "completed_message_retries":foreground[received_start..received_end].iter().map(|s|s.retries).sum::<u64>()}));
        offset = end_offset;
    }
    let mut maintenance = BTreeMap::new();
    for (class, worker, period) in [
        ("projection", arrivals.workers, plan.projection_interval_ns),
        (
            "housekeeping",
            arrivals.workers + 1,
            plan.housekeeping_interval_ns,
        ),
    ] {
        let selected: BTreeMap<_, _> = samples
            .iter()
            .filter(|s| s.class == class)
            .map(|s| (s.ordinal, s))
            .collect();
        let mut pending_due = 0;
        let mut late_completed = 0;
        let mut unfinished_past_deadline = 0;
        let mut jobs = Vec::new();
        for ordinal in 0..offered_by_worker[worker] {
            let scheduled = arrivals
                .start_ns
                .checked_add(
                    (ordinal + 1)
                        .checked_mul(period)
                        .ok_or("capacity maintenance schedule overflow")?,
                )
                .ok_or("capacity timer clock overflow")?;
            let deadline = scheduled
                .checked_add(period)
                .ok_or("capacity timer deadline overflow")?;
            let sample = selected.get(&ordinal);
            let due = scheduled < observed_until_ns;
            let late = sample.is_some_and(|s| s.received_ns >= deadline);
            let overdue = sample.is_none() && deadline <= observed_until_ns;
            pending_due += u64::from(due && sample.is_none());
            late_completed += u64::from(late);
            unfinished_past_deadline += u64::from(overdue);
            jobs.push(json!({"ordinal":ordinal,"scheduled_ns":scheduled,"deadline_ns":deadline,
                "started_ns":sample.map(|s|s.started_ns),"finished_ns":sample.map(|s|s.finished_ns),
                "received_ns":sample.map(|s|s.received_ns),"retries":sample.map(|s|s.retries),
                "positive_effect":sample.map(|s|s.positive_effect),
                "completed":sample.is_some(),"due_at_observation":due,
                "completed_after_next_tick":late,"unfinished_past_next_tick":overdue,
                "state":if sample.is_some(){"completed"}else if due{"awaiting_completion_or_receipt"}else{"not_yet_due"}}));
        }
        maintenance.insert(class, json!({"offered":offered_by_worker[worker],"completed":selected.len(),
            "positive_effect_jobs":selected.values().filter(|s|s.positive_effect).count(),
            "pending_due":pending_due,"completed_after_next_tick":late_completed,
            "unfinished_past_next_tick":unfinished_past_deadline,
            "p99_us_including_retries":quantile_99(selected.values().map(|s|s.received_ns-s.scheduled_ns).collect()),
            "jobs":jobs}));
    }
    Ok(
        json!({"format":"capacity-accounting-v1","admission_start_ns":arrivals.start_ns,
        "admission_end_ns":end,"observed_until_ns":observed_until_ns,
        "execution_completed":execution_completed,"admission_fully_observed":fully_observed,
        "foreground":{"offered":offered,"offered_observed":arrivals_before(arrivals,observed_until_ns),
            "received_in_admission":in_admission,"in_admission_counts_final":fully_observed,
            "completion_rate_in_admission":fully_observed.then(||in_admission as f64 * SECOND as f64 / arrivals.duration_ns as f64),
            "completed_total":foreground.len(),"remaining_at_admission_end":fully_observed.then(||offered-in_admission as u64),
            "p99_us_including_retries":quantile_99(foreground.iter().map(|s|s.received_ns-s.scheduled_ns).collect()),
            "in_admission_p99_us_including_retries":quantile_99(foreground[..in_admission].iter().map(|s|s.received_ns-s.scheduled_ns).collect())},
        "window_ns":SECOND,"windows":windows,"maintenance":maintenance,
        "completion_clock":"coordinator_received_ns",
        "interval_convention":"Admission and windows are half-open [start,end); exact-end completions belong to drain or the next window. Partial windows stop at observed_until_ns.",
        "queue_scope":"Scheduled foreground arrivals minus successful logical-input receipts; includes waiting, executing/retrying and result IPC. Oldest age is at the observed window end.",
        "latency_scope":"Scheduled arrival through coordinator receipt including retries, backoff, queueing and result IPC. Arrival-cohort percentiles include subsequent drain; incomplete cohorts are explicitly counted.",
        "maintenance_deadline_policy":"Diagnostic completion strictly before the next cadence tick, including deadlines after admission; a job completes only after its terminal transaction in sweep mode. No production deadline is inferred.",
        "partial_scope":"Only validated receipts received by observation are represented; running peers may have committed additional work. Failed-attempt retries remain in failure evidence, not completed-message retry counts.",
        "count_scope":"Each admitted foreground input counts once. Fork updates, attempts, and maintenance batch transactions do not inflate foreground arrivals/completions. Maintenance counts whole scheduled jobs."}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    const START: u64 = 10 * SECOND;

    fn plan() -> Plan {
        Plan {
            arrivals: ArrivalPlan {
                start_ns: START,
                duration_ns: 4 * SECOND,
                rate_per_second: 2,
                workers: 1,
            },
            projection_interval_ns: SECOND,
            housekeeping_interval_ns: 2 * SECOND,
        }
    }

    fn event(
        worker: usize,
        class: &'static str,
        ordinal: u64,
        scheduled: u64,
        received: u64,
    ) -> ExecutionSample {
        ExecutionSample {
            worker,
            class,
            ordinal,
            scheduled_ns: START + scheduled,
            started_ns: START + scheduled + 1,
            finished_ns: START + scheduled + 2,
            received_ns: START + received,
            retries: 3,
            positive_effect: true,
        }
    }

    fn complete_samples() -> Vec<ExecutionSample> {
        let mut samples: Vec<_> = (0..8)
            .map(|q| {
                event(
                    0,
                    "foreground",
                    q,
                    q * SECOND / 2,
                    q * SECOND / 2 + 10_000_000,
                )
            })
            .collect();
        samples.extend((0..3).map(|q| {
            event(
                1,
                "projection",
                q,
                (q + 1) * SECOND,
                (q + 1) * SECOND + 50_000_000,
            )
        }));
        samples.push(event(
            2,
            "housekeeping",
            0,
            2 * SECOND,
            2 * SECOND + 50_000_000,
        ));
        samples
    }

    #[test]
    fn admission_excludes_exact_end_completion_but_cohort_latency_includes_drain() {
        let mut samples = complete_samples();
        samples[7].received_ns = START + 4 * SECOND;
        samples[7].retries = 128;
        let r = summarize(&plan(), &samples, &[8, 3, 1], START + 5 * SECOND, true).unwrap();
        assert_eq!(r["foreground"]["offered"], 8);
        assert_eq!(r["foreground"]["completed_total"], 8);
        assert_eq!(r["foreground"]["received_in_admission"], 7);
        assert_eq!(r["foreground"]["remaining_at_admission_end"], 1);
        assert_eq!(r["foreground"]["completion_rate_in_admission"], 1.75);
        assert_eq!(r["foreground"]["p99_us_including_retries"], 500_000.0);
        assert_eq!(
            r["foreground"]["in_admission_p99_us_including_retries"],
            10_000.0
        );
        assert_eq!(r["windows"][3]["completions"], 1);
        assert_eq!(r["windows"][3]["backlog_end"], 1);
        assert_eq!(r["windows"][3]["arrival_cohort_completed"], 2);
        assert_eq!(r["windows"][3]["arrival_cohort_unfinished"], 0);
        assert_eq!(
            r["windows"][3]["arrival_cohort_p99_us_including_retries"],
            500_000.0
        );
        assert_eq!(r["maintenance"]["projection"]["completed"], 3);
    }

    #[test]
    fn failed_run_reports_only_observed_windows_and_censors_admission_totals() {
        let mut samples: Vec<_> = complete_samples().into_iter().take(4).collect();
        samples.push(event(1, "projection", 0, SECOND, SECOND + 50_000_000));
        let r = summarize(
            &plan(),
            &samples,
            &[8, 3, 1],
            START + 2 * SECOND + 250_000_000,
            false,
        )
        .unwrap();
        assert_eq!(r["execution_completed"], false);
        assert_eq!(r["foreground"]["offered"], 8);
        assert_eq!(r["foreground"]["offered_observed"], 5);
        assert_eq!(r["foreground"]["received_in_admission"], 4);
        assert_eq!(r["foreground"]["in_admission_counts_final"], false);
        assert!(r["foreground"]["completion_rate_in_admission"].is_null());
        assert!(r["foreground"]["remaining_at_admission_end"].is_null());
        assert_eq!(r["windows"].as_array().unwrap().len(), 3);
        assert_eq!(r["windows"][2]["fully_observed"], false);
        assert_eq!(r["windows"][2]["planned_arrivals"], 2);
        assert_eq!(r["windows"][2]["arrivals"], 1);
        assert_eq!(r["windows"][2]["backlog_end"], 1);
        assert_eq!(r["windows"][2]["oldest_uncompleted_age_ns"], 250_000_000);
        assert!(r["windows"][2]["arrival_cohort_p99_us_including_retries"].is_null());
        assert_eq!(r["maintenance"]["projection"]["pending_due"], 1);
        assert_eq!(
            r["maintenance"]["projection"]["jobs"][2]["state"],
            "not_yet_due"
        );
        assert!(r["maintenance"]["housekeeping"]["jobs"][0]["received_ns"].is_null());
    }

    #[test]
    fn next_tick_deadline_is_strict_and_unfinished_jobs_are_not_successes() {
        let sample = event(1, "projection", 0, SECOND, 2 * SECOND);
        let r = summarize(&plan(), &[sample], &[8, 3, 1], START + 4 * SECOND, false).unwrap();
        assert_eq!(
            r["maintenance"]["projection"]["completed_after_next_tick"],
            1
        );
        assert_eq!(
            r["maintenance"]["projection"]["unfinished_past_next_tick"],
            2
        );
        assert_eq!(r["maintenance"]["projection"]["positive_effect_jobs"], 1);
        assert_eq!(
            r["maintenance"]["housekeeping"]["unfinished_past_next_tick"],
            1
        );
        assert_eq!(r["foreground"]["completed_total"], 0);
        assert_eq!(r["windows"][0]["completions"], 0);
        assert!(r["foreground"]["p99_us_including_retries"].is_null());
    }

    #[test]
    fn oldest_outstanding_tracks_missing_earlier_input_not_receipt_order() {
        let mut p = plan();
        p.arrivals.workers = 2;
        p.arrivals.duration_ns = 2 * SECOND;
        let samples = vec![
            event(1, "foreground", 1, SECOND / 2, SECOND / 2 + 10_000_000),
            event(0, "foreground", 0, 0, SECOND + 200_000_000),
            event(2, "projection", 0, SECOND, SECOND + 50_000_000),
            event(0, "foreground", 2, SECOND, SECOND + 300_000_000),
            event(
                1,
                "foreground",
                3,
                3 * SECOND / 2,
                3 * SECOND / 2 + 10_000_000,
            ),
        ];
        let r = summarize(&p, &samples, &[2, 2, 1, 0], START + 3 * SECOND, true).unwrap();
        assert_eq!(r["windows"][0]["cumulative_completions"], 1);
        assert_eq!(r["windows"][0]["backlog_end"], 1);
        assert_eq!(r["windows"][0]["oldest_uncompleted_age_ns"], SECOND);
        assert_eq!(r["windows"][0]["arrival_cohort_completed"], 2);
        assert_eq!(r["windows"][1]["completions"], 3);
        assert!(r["windows"][1]["oldest_uncompleted_age_ns"].is_null());
    }

    #[test]
    fn duplicate_logical_input_wrong_role_out_of_range_and_future_receipts_fail() {
        let offer = [8, 3, 1];
        let cutoff = START + 5 * SECOND;
        assert!(summarize(
            &plan(),
            &[
                event(0, "foreground", 0, 0, 10),
                event(0, "foreground", 0, 0, 20)
            ],
            &offer,
            cutoff,
            false
        )
        .is_err());
        assert!(summarize(
            &plan(),
            &[event(1, "foreground", 0, 0, 10)],
            &offer,
            cutoff,
            false
        )
        .is_err());
        assert!(summarize(
            &plan(),
            &[event(3, "housekeeping", 0, 2 * SECOND, 3 * SECOND)],
            &offer,
            cutoff,
            false
        )
        .is_err());
        assert!(summarize(
            &plan(),
            &[event(0, "foreground", 8, 4 * SECOND, 4 * SECOND + 10)],
            &offer,
            cutoff,
            false
        )
        .is_err());
        assert!(summarize(
            &plan(),
            &[event(0, "foreground", 0, 0, 6 * SECOND)],
            &offer,
            cutoff,
            false
        )
        .is_err());
        assert!(summarize(
            &plan(),
            &[event(0, "foreground", 1, 0, 10)],
            &offer,
            cutoff,
            false
        )
        .is_err());
    }

    #[test]
    fn completed_accounting_requires_every_foreground_input_and_whole_timer_job() {
        let samples = complete_samples();
        assert!(summarize(&plan(), &samples[..8], &[8, 3, 1], START + 5 * SECOND, true).is_err());
        assert!(summarize(&plan(), &samples[1..], &[8, 3, 1], START + 5 * SECOND, true).is_err());
        assert!(summarize(&plan(), &samples, &[9, 3, 1], START + 5 * SECOND, true).is_err());
        assert!(summarize(&plan(), &samples, &[8, 4, 1], START + 5 * SECOND, true).is_err());
    }

    #[test]
    fn partial_last_second_and_absent_maintenance_keep_exact_denominators() {
        let mut p = plan();
        p.arrivals.duration_ns = SECOND / 2;
        let r = summarize(
            &p,
            &[event(0, "foreground", 0, 0, 10_000_000)],
            &[1, 0, 0],
            START + SECOND,
            true,
        )
        .unwrap();
        assert_eq!(r["windows"].as_array().unwrap().len(), 1);
        assert_eq!(r["windows"][0]["end_offset_ns"], SECOND / 2);
        assert_eq!(r["foreground"]["completion_rate_in_admission"], 2.0);
        assert_eq!(r["maintenance"]["projection"]["offered"], 0);
    }

    #[test]
    fn fractional_arrivals_exact_window_boundary_and_prestart_observation() {
        let mut p = plan();
        p.arrivals.duration_ns = 2 * SECOND;
        p.arrivals.rate_per_second = 3;
        let mut samples: Vec<_> = (0..6)
            .map(|q| {
                let at = q * SECOND / 3;
                event(0, "foreground", q, at, at + 10)
            })
            .collect();
        samples[2].received_ns = START + SECOND;
        samples.push(event(1, "projection", 0, SECOND, SECOND + 10));
        let r = summarize(&p, &samples, &[6, 1, 0], START + 3 * SECOND, true).unwrap();
        assert_eq!(r["windows"][0]["arrivals"], 3);
        assert_eq!(r["windows"][0]["completions"], 2);
        assert_eq!(r["windows"][1]["completions"], 4);
        let r = summarize(&p, &[], &[6, 1, 0], START - 1, false).unwrap();
        assert!(r["windows"].as_array().unwrap().is_empty());
        assert_eq!(r["foreground"]["offered_observed"], 0);
    }

    #[test]
    fn progress_reports_logical_foreground_receipts_only() {
        let p = plan();
        let r = progress(&p.arrivals, 1, START + SECOND);
        assert_eq!(r["foreground_arrivals"], 2);
        assert_eq!(r["foreground_completed_received"], 1);
        assert_eq!(r["foreground_outstanding"], 1);
        assert_eq!(progress(&p.arrivals, 0, START)["foreground_arrivals"], 0);
    }
}
