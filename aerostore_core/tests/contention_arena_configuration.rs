//! Exercise the same CLI/configuration and metadata code used by the benchmark.
#![cfg(target_os = "linux")]
#![recursion_limit = "256"]
#![allow(dead_code, unused_imports)]
#[path = "../benches/contention_crucible/mod.rs"]
mod contention_crucible;
#[path = "../benches/extended_crucible/mod.rs"]
mod extended_crucible;
