//! Evidence-storage resource caps; they must never truncate an offered corpus.
pub const FULL_MESSAGES_PER_WORKER: usize = 100_000;
pub const METRICS_MESSAGES_PER_WORKER: usize = 1_000_000;

pub fn message_cap_allowed(workload: &str, evidence: &str, mode: &str, cap: usize) -> bool {
    let maximum = if workload == "calibrated" && evidence == "metrics" && mode == "sustained" {
        METRICS_MESSAGES_PER_WORKER
    } else {
        FULL_MESSAGES_PER_WORKER
    };
    (1..=maximum).contains(&cap)
}

/// Called before any Run/Calibrated request is dispatched to ready workers.
/// Checking each actual owner matters: an average does not bound affinity skew.
pub fn validate_offered(offered_by_worker: &[u64], max_messages: usize) -> Result<(), String> {
    if offered_by_worker.iter().any(|offered| *offered > max_messages as u64) {
        return Err("calibrated offered corpus exceeds --max-messages for a worker; no jobs may be truncated".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn larger_cap_is_explicit_calibrated_sustained_metrics_only() {
        for workload in ["legacy", "lifecycle", "fleet", "calibrated"] {
            for evidence in ["full", "metrics"] {
                for mode in ["all", "scenarios", "sustained", "serve"] {
                    assert!(message_cap_allowed(workload, evidence, mode, 100_000));
                    let enlarged = workload == "calibrated" && evidence == "metrics" && mode == "sustained";
                    assert_eq!(message_cap_allowed(workload, evidence, mode, 100_001), enlarged);
                    assert_eq!(message_cap_allowed(workload, evidence, mode, 1_000_000), enlarged);
                    for cap in [0, 1_000_001, usize::MAX] {
                        assert!(!message_cap_allowed(workload, evidence, mode, cap));
                    }
                }
            }
        }
    }

    #[test]
    fn exact_worker_caps_include_affinity_skew_and_timer_workers() {
        // The foreground average fits, but the actual first owner does not.
        for counts in [vec![101, 99, 3, 3], vec![100, 100, 101, 3], vec![100, 100, 3, 101]] {
            let original = counts.clone();
            let error = validate_offered(&counts, 100).unwrap_err();
            assert!(error.contains("no jobs may be truncated"));
            assert_eq!(counts, original);
        }
        assert!(validate_offered(&[100, 0, 3, 3], 100).is_ok());
        assert!(validate_offered(&[1_000_000, 0, 3, 3], 1_000_000).is_ok());
        assert!(validate_offered(&[1_000_001, 0, 3, 3], 1_000_000).is_err());
    }
}
