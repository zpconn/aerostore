use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub struct RetryPolicy {
    pub base_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub escalate_after_failures: u32,
    pub max_retries_per_unit: u32,
}

impl RetryPolicy {
    pub const ESCALATE_AFTER_FAILURES: u32 = 3;
    pub const BASE_BACKOFF_MS: u64 = 1;
    pub const MAX_BACKOFF_MS: u64 = 16;
    pub const MAX_RETRIES_PER_UNIT: u32 = 64;

    pub const fn hot_key_default() -> Self {
        Self {
            base_backoff_ms: Self::BASE_BACKOFF_MS,
            max_backoff_ms: Self::MAX_BACKOFF_MS,
            escalate_after_failures: Self::ESCALATE_AFTER_FAILURES,
            max_retries_per_unit: Self::MAX_RETRIES_PER_UNIT,
        }
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::hot_key_default()
    }
}

pub struct RetryBackoff {
    policy: RetryPolicy,
    rng_state: u64,
}

impl RetryBackoff {
    pub fn with_seed(seed: u64, policy: RetryPolicy) -> Self {
        let mut seeded = seed ^ 0x9E37_79B9_7F4A_7C15_u64;
        if seeded == 0 {
            seeded = 1;
        }
        Self {
            policy,
            rng_state: seeded,
        }
    }

    #[inline]
    pub fn policy(&self) -> RetryPolicy {
        self.policy
    }

    pub fn next_delay(&mut self, attempt: u32) -> Duration {
        let nominal_ms = self.nominal_ms(attempt);
        let nominal_us = nominal_ms.saturating_mul(1_000);
        let min_us = (nominal_us / 2).max(100);
        let max_us = nominal_us.saturating_add(nominal_us / 2).max(min_us);
        let span = max_us.saturating_sub(min_us).saturating_add(1);
        let jitter = self.next_u64() % span;
        Duration::from_micros(min_us.saturating_add(jitter))
    }

    pub fn sleep_for_attempt(&mut self, attempt: u32) -> Duration {
        let delay = self.next_delay(attempt);
        std::thread::sleep(delay);
        delay
    }

    fn nominal_ms(&self, attempt: u32) -> u64 {
        let shift = attempt.min(31);
        let scaled = self
            .policy
            .base_backoff_ms
            .saturating_mul(1_u64 << shift)
            .max(self.policy.base_backoff_ms);
        scaled.min(self.policy.max_backoff_ms)
    }

    #[inline]
    fn next_u64(&mut self) -> u64 {
        let mut x = self.rng_state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        if x == 0 {
            x = 1;
        }
        self.rng_state = x;
        x
    }
}

#[cfg(test)]
mod tests {
    use super::{RetryBackoff, RetryPolicy};
    use std::time::Duration;

    #[test]
    fn nominal_backoff_is_truncated() {
        let policy = RetryPolicy::hot_key_default();
        let mut backoff = RetryBackoff::with_seed(1, policy);

        let d0 = backoff.next_delay(0);
        let d1 = backoff.next_delay(1);
        let d8 = backoff.next_delay(8);

        assert!(d0.as_millis() <= policy.max_backoff_ms as u128);
        assert!(d1.as_millis() <= policy.max_backoff_ms as u128);
        assert!(d8.as_millis() <= policy.max_backoff_ms as u128);
    }

    #[test]
    fn next_delay_respects_jitter_bounds_for_each_attempt() {
        let policy = RetryPolicy::hot_key_default();
        let mut backoff = RetryBackoff::with_seed(11, policy);

        for attempt in [0_u32, 1, 2, 4, 8, 32] {
            let nominal_ms = policy
                .base_backoff_ms
                .saturating_mul(1_u64 << attempt.min(31))
                .min(policy.max_backoff_ms);
            let nominal_us = nominal_ms * 1_000;
            let min_us = (nominal_us / 2).max(100);
            let max_us = nominal_us + (nominal_us / 2);

            let delay = backoff.next_delay(attempt);
            let delay_us = delay.as_micros() as u64;
            assert!(delay_us >= min_us, "delay {} < min {}", delay_us, min_us);
            assert!(delay_us <= max_us, "delay {} > max {}", delay_us, max_us);
        }
    }

    #[test]
    fn same_seed_produces_same_delay_sequence() {
        let policy = RetryPolicy::hot_key_default();
        let mut a = RetryBackoff::with_seed(42, policy);
        let mut b = RetryBackoff::with_seed(42, policy);

        for attempt in 0..32_u32 {
            assert_eq!(a.next_delay(attempt), b.next_delay(attempt));
        }
    }

    #[test]
    fn different_seed_produces_divergent_sequence() {
        let policy = RetryPolicy::hot_key_default();
        let mut a = RetryBackoff::with_seed(42, policy);
        let mut b = RetryBackoff::with_seed(43, policy);
        let mut any_diff = false;

        for attempt in 0..32_u32 {
            if a.next_delay(attempt) != b.next_delay(attempt) {
                any_diff = true;
                break;
            }
        }
        assert!(any_diff, "expected at least one delay to differ");
    }

    #[test]
    fn delay_is_truncated_at_policy_max() {
        let policy = RetryPolicy {
            base_backoff_ms: 4,
            max_backoff_ms: 8,
            escalate_after_failures: RetryPolicy::ESCALATE_AFTER_FAILURES,
            max_retries_per_unit: RetryPolicy::MAX_RETRIES_PER_UNIT,
        };
        let mut backoff = RetryBackoff::with_seed(5, policy);
        let max = Duration::from_millis(policy.max_backoff_ms + (policy.max_backoff_ms / 2));

        for attempt in 0..64_u32 {
            assert!(backoff.next_delay(attempt) <= max);
        }
    }
}
