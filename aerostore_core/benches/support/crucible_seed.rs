//! Optional reproducible per-worker row-choice seeds. This does not fix thread
//! scheduling, timestamp assignment, retry outcomes, or complete histories.
use std::ffi::OsStr;

pub const SEED_ENV: &str = "AEROSTORE_CRUCIBLE_SEED";
pub const FIXED_SEED_ALGORITHM: &str = "worker_add_xorshift64_v1";

/// No seed preserves entropy-based behavior; a provided invalid seed is an error.
/// Parsing is pure so tests never mutate process-global environment variables.
pub fn parse_seed(raw: Option<&OsStr>) -> Result<Option<u64>, &'static str> {
    let Some(raw) = raw else { return Ok(None) };
    let text = raw.to_str().ok_or("expected an ASCII decimal u64")?;
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("expected an ASCII decimal u64");
    }
    text.parse::<u64>()
        .map(Some)
        .map_err(|_| "decimal seed exceeds u64::MAX")
}

#[inline]
pub fn fixed_worker_seed(seed: u64, worker: u64, salt: u64) -> u64 {
    let state = worker
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(seed)
        .wrapping_add(salt);
    if state == 0 {
        1
    } else {
        state
    }
}

/// Original Crucible generator, shared with seed tests without changing draws.
#[inline]
pub fn next_u64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    if x == 0 {
        x = 1;
    }
    *state = x;
    x
}
