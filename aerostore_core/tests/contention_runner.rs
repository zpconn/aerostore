//! Standalone supervision and pacing tests; no engine/model fixtures imported.
#![cfg(target_os = "linux")]
#![allow(dead_code)]

#[path = "../benches/contention_crucible/supervision.rs"]
mod supervision;

#[path = "../benches/contention_crucible/corpus_limits.rs"]
mod corpus_limits;
