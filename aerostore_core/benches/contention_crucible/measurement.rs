//! Arrival schedules are independent of completions. No generator thread or
//! unbounded queue is needed: the offered corpus is a deterministic timeline.
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// Observed execution intervals include retries/backoff, but exclude admission
/// queueing. They measure occupied worker time, not CPU utilization.
pub struct ExecutionSample {
    pub worker: usize,
    pub class: &'static str,
    pub scheduled_ns: u64,
    pub started_ns: u64,
    pub finished_ns: u64,
    pub received_ns: u64,
    pub retries: u64,
    pub positive_effect: bool,
}

#[derive(Default)]
pub struct FlightOrderAudit {
    last: BTreeMap<usize, (u64, u64)>,
    completions: u64,
}
impl FlightOrderAudit {
    pub fn observe(
        &mut self,
        identity: usize,
        ordinal: u64,
        started_ns: u64,
        finished_ns: u64,
    ) -> Result<(), String> {
        if started_ns >= finished_ns {
            return Err("invalid foreground execution interval".into());
        }
        if let Some((previous, finished)) = self.last.get(&identity) {
            if previous.checked_add(1) != Some(ordinal) || started_ns < *finished {
                return Err(format!(
                    "foreground flight {identity} was reordered or overlapped"
                ));
            }
        } else if ordinal != 0 {
            return Err(format!(
                "foreground flight {identity} omitted its first offered input"
            ));
        }
        self.last.insert(identity, (ordinal, finished_ns));
        self.completions += 1;
        Ok(())
    }

    pub fn report(&self) -> Value {
        json!({"checked":true,"passed":true,"foreground_completions":self.completions,
            "identities_observed":self.last.len(),
            "scope":"Each flight's offered foreground order and nonoverlapping first-attempt through successful-completion intervals; declared profile assumption, not an inferred production contract."})
    }
}

fn quantile_99(mut values: Vec<u64>) -> Option<f64> {
    values.sort_unstable();
    (!values.is_empty()).then(|| values[(values.len() * 99).div_ceil(100) - 1] as f64 / 1000.)
}

fn overlapping_count(subjects: &[(u64, u64)], others: &[(u64, u64)]) -> usize {
    let mut ordered = others.to_vec();
    ordered.sort_unstable();
    let mut maximum_end = 0;
    let prefix_ends: Vec<_> = ordered
        .iter()
        .map(|(_, end)| {
            maximum_end = maximum_end.max(*end);
            maximum_end
        })
        .collect();
    subjects
        .iter()
        .filter(|(start, end)| {
            let before = ordered.partition_point(|(other_start, _)| other_start < end);
            before > 0 && prefix_ends[before - 1] > *start
        })
        .count()
}

pub fn calibrated_execution_summary(
    samples: &[ExecutionSample],
    offered_by_worker: &[u64],
    foreground_workers: usize,
    elapsed_ns: u64,
) -> Result<Value, String> {
    if elapsed_ns == 0
        || foreground_workers == 0
        || offered_by_worker.len() != foreground_workers + 2
    {
        return Err("invalid calibrated worker accounting dimensions".into());
    }
    let role = |worker: usize| {
        if worker < foreground_workers {
            "foreground"
        } else if worker == foreground_workers {
            "projection"
        } else {
            "housekeeping"
        }
    };
    let mut previous_finish = vec![0; offered_by_worker.len()];
    let mut observed = vec![0_u64; offered_by_worker.len()];
    for sample in samples {
        if sample.worker >= offered_by_worker.len()
            || sample.class != role(sample.worker)
            || !(sample.scheduled_ns <= sample.started_ns
                && sample.started_ns < sample.finished_ns
                && sample.finished_ns <= sample.received_ns)
        {
            return Err("invalid calibrated execution sample".into());
        }
        if sample.started_ns < previous_finish[sample.worker] {
            return Err("overlapping execution intervals from one calibrated worker".into());
        }
        previous_finish[sample.worker] = sample.finished_ns;
        observed[sample.worker] += 1;
    }
    if observed != offered_by_worker {
        return Err("calibrated execution samples do not cover every offered job".into());
    }
    let activity: Vec<_> = offered_by_worker
        .iter()
        .enumerate()
        .map(|(worker, offered)| {
            let selected: Vec<_> = samples.iter().filter(|s| s.worker == worker).collect();
            let busy: u64 = selected.iter().map(|s| s.finished_ns - s.started_ns).sum();
            json!({"worker":worker,"role":role(worker),"offered":offered,"completed":selected.len(),
            "retries":selected.iter().map(|s|s.retries).sum::<u64>(),"busy_ns":busy,
            "utilization":busy as f64 / elapsed_ns as f64})
        })
        .collect();
    let mut classes = BTreeMap::new();
    for class in ["foreground", "projection", "housekeeping", "maintenance"] {
        let include = |candidate: &str| {
            candidate == class || (class == "maintenance" && candidate != "foreground")
        };
        let selected: Vec<_> = samples.iter().filter(|s| include(s.class)).collect();
        let offered: u64 = offered_by_worker
            .iter()
            .enumerate()
            .filter(|(worker, _)| include(role(*worker)))
            .map(|(_, n)| n)
            .sum();
        classes.insert(class, json!({"offered":offered,"completed":selected.len(),
            "retries":selected.iter().map(|s|s.retries).sum::<u64>(),
            "positive_effect_jobs":selected.iter().filter(|s|s.positive_effect).count(),
            "p99_us_including_retries":quantile_99(selected.iter().map(|s|s.received_ns-s.scheduled_ns).collect()),
            "service_latency_p99_us_including_retries":quantile_99(selected.iter().map(|s|s.finished_ns-s.started_ns).collect()),
            "arrival_queue_delay_p99_us":quantile_99(selected.iter().map(|s|s.started_ns-s.scheduled_ns).collect())}));
    }
    let foreground: Vec<_> = samples
        .iter()
        .filter(|s| s.class == "foreground")
        .map(|s| (s.started_ns, s.finished_ns))
        .collect();
    let maintenance: Vec<_> = samples
        .iter()
        .filter(|s| s.class != "foreground")
        .map(|s| (s.started_ns, s.finished_ns))
        .collect();
    let min_busy = activity
        .iter()
        .map(|v| v["busy_ns"].as_u64().unwrap())
        .min()
        .unwrap();
    let max_busy = activity
        .iter()
        .map(|v| v["busy_ns"].as_u64().unwrap())
        .max()
        .unwrap();
    Ok(
        json!({"workload_classes":classes,"worker_activity":activity,
        "worker_utilization_scope":"Wall time from first attempt through successful completion, including retries/backoff, divided by continuous admission-to-drain duration; not CPU usage.",
        "worker_skew":{"minimum_busy_ns":min_busy,"maximum_busy_ns":max_busy},
        "execution_overlap":{"foreground_messages_overlapping_maintenance":overlapping_count(&foreground,&maintenance),
            "maintenance_jobs_overlapping_foreground":overlapping_count(&maintenance,&foreground),
            "scope":"First-attempt through successful-completion intervals including retries/backoff; not proof that particular database queries overlap."}}),
    )
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArrivalPlan {
    pub start_ns: u64,
    pub duration_ns: u64,
    pub rate_per_second: u64,
    pub workers: usize,
}

impl ArrivalPlan {
    pub fn offered(&self) -> u64 {
        ((self.duration_ns as u128 * self.rate_per_second as u128).div_ceil(1_000_000_000)) as u64
    }

    pub fn scheduled_ns(&self, sequence: u64) -> Option<u64> {
        if self.rate_per_second == 0 || sequence >= self.offered() {
            return None;
        }
        let offset = sequence as u128 * 1_000_000_000 / self.rate_per_second as u128;
        self.start_ns.checked_add(u64::try_from(offset).ok()?)
    }

    pub fn worker_offered(&self, worker: usize) -> u64 {
        if worker >= self.workers || self.offered() <= worker as u64 {
            return 0;
        }
        (self.offered() - 1 - worker as u64) / self.workers as u64 + 1
    }

    /// Includes the next message waiting to run. Arrivals at the exact end of
    /// the admission interval are excluded; all admitted messages must drain.
    pub fn backlog(&self, worker: usize, completed: u64, now_ns: u64) -> u64 {
        if now_ns < self.start_ns || self.workers == 0 || worker >= self.workers {
            return 0;
        }
        let elapsed = now_ns.saturating_sub(self.start_ns);
        // ceil((elapsed+1)*rate / 1e9) counts integer-nanosecond arrivals <= now.
        let due = (((elapsed as u128 + 1) * self.rate_per_second as u128).div_ceil(1_000_000_000))
            .min(self.offered() as u128) as u64;
        let assigned = if due <= worker as u64 {
            0
        } else {
            (due - 1 - worker as u64) / self.workers as u64 + 1
        };
        assigned.saturating_sub(completed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flight_order_accepts_cross_flight_overlap_and_rejects_same_flight_reordering() {
        let mut order = FlightOrderAudit::default();
        assert!(order.observe(1, 1, 100, 200).is_err());
        order.observe(1, 0, 100, 200).unwrap();
        order.observe(2, 0, 150, 250).unwrap();
        assert!(order.observe(1, 2, 200, 300).is_err());
        assert!(order.observe(1, 1, 199, 300).is_err());
        order.observe(1, 1, 200, 300).unwrap();
        assert_eq!(order.report()["foreground_completions"], 3);
        assert_eq!(order.report()["identities_observed"], 2);
    }

    #[test]
    fn class_metrics_include_queue_retry_and_receipt_delay_and_report_idle_jobs() {
        let samples = vec![
            ExecutionSample {
                worker: 0,
                class: "foreground",
                scheduled_ns: 1000,
                started_ns: 3000,
                finished_ns: 6000,
                received_ns: 7000,
                retries: 2,
                positive_effect: true,
            },
            ExecutionSample {
                worker: 1,
                class: "projection",
                scheduled_ns: 2000,
                started_ns: 4000,
                finished_ns: 5000,
                received_ns: 5500,
                retries: 1,
                positive_effect: true,
            },
        ];
        let r = calibrated_execution_summary(&samples, &[1, 1, 0], 1, 10_000).unwrap();
        assert_eq!(
            r["workload_classes"]["foreground"]["p99_us_including_retries"],
            6.0
        );
        assert_eq!(
            r["workload_classes"]["foreground"]["service_latency_p99_us_including_retries"],
            3.0
        );
        assert_eq!(
            r["workload_classes"]["foreground"]["arrival_queue_delay_p99_us"],
            2.0
        );
        assert_eq!(r["workload_classes"]["foreground"]["retries"], 2);
        assert_eq!(r["workload_classes"]["maintenance"]["completed"], 1);
        assert!(r["workload_classes"]["housekeeping"]["p99_us_including_retries"].is_null());
        assert_eq!(r["worker_activity"][2]["utilization"], 0.0);
        assert_eq!(r["worker_activity"][0]["utilization"], 0.3);
        assert_eq!(
            r["execution_overlap"]["foreground_messages_overlapping_maintenance"],
            1
        );
        assert_eq!(
            r["execution_overlap"]["maintenance_jobs_overlapping_foreground"],
            1
        );
        assert!(calibrated_execution_summary(&samples, &[2, 1, 0], 1, 10_000).is_err());
    }

    #[test]
    fn interval_overlap_checks_half_open_boundaries_and_long_enclosing_work() {
        assert_eq!(
            overlapping_count(&[(10, 20), (30, 40), (50, 60)], &[(20, 30)]),
            0
        );
        assert_eq!(
            overlapping_count(&[(10, 20), (30, 40), (50, 60)], &[(0, 55), (1, 2)]),
            3
        );
        assert_eq!(overlapping_count(&[(10, 20)], &[]), 0);
    }

    #[test]
    fn fixed_corpus_is_partitioned_without_omission_or_duplication() {
        for workers in 1..9 {
            let plan = ArrivalPlan {
                start_ns: 100,
                duration_ns: 2_000_000_000,
                rate_per_second: 7,
                workers,
            };
            assert_eq!(plan.offered(), 14);
            let mut sequences = Vec::new();
            for worker in 0..workers {
                for local in 0..plan.worker_offered(worker) {
                    sequences.push(local * workers as u64 + worker as u64);
                }
            }
            sequences.sort();
            assert_eq!(sequences, (0..14).collect::<Vec<_>>());
            assert!(plan.scheduled_ns(14).is_none());
        }
    }

    #[test]
    fn slow_completion_accumulates_visible_backlog_instead_of_throttling_arrivals() {
        let plan = ArrivalPlan {
            start_ns: 10,
            duration_ns: 1_000_000_000,
            rate_per_second: 10,
            workers: 2,
        };
        assert_eq!(plan.backlog(0, 0, 9), 0);
        assert_eq!(plan.backlog(0, 0, 10), 1);
        assert_eq!(plan.backlog(1, 0, 10), 0);
        assert_eq!(plan.backlog(0, 1, 900_000_010), 4);
        assert_eq!(plan.backlog(1, 1, u64::MAX), 4);
        assert_eq!(plan.backlog(0, 5, u64::MAX), 0);
    }

    #[test]
    fn fractional_periods_and_admission_boundary_agree() {
        let plan = ArrivalPlan {
            start_ns: 0,
            duration_ns: 1_000_000_000,
            rate_per_second: 3,
            workers: 1,
        };
        assert_eq!(plan.scheduled_ns(1), Some(333_333_333));
        assert_eq!(plan.backlog(0, 0, 333_333_332), 1);
        assert_eq!(plan.backlog(0, 0, 333_333_333), 2);
        assert_eq!(plan.backlog(0, 0, 1_000_000_000), 3);
    }
}
