//! Standalone supervision and pacing tests; no engine/model fixtures imported.
#![cfg(target_os = "linux")]
#![allow(dead_code)]

#[path = "../benches/contention_crucible/supervision.rs"]
mod supervision;
