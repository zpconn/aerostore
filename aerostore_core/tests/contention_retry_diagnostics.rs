//! Worker retry bookkeeping over a mock Store and service protocol regressions.
//! Module filters select focused tests. Running every imported debug test needs
//! RUST_MIN_STACK=16777216 for the existing native attachment fixtures.
#![allow(dead_code, unused_imports)]
#[path = "../benches/extended_crucible"]
mod extended_crucible {
    #[path = "aerostore.rs"]
    pub mod aerostore;
    #[path = "metrics.rs"]
    pub mod metrics;
    #[path = "model.rs"]
    pub mod model;
}
#[path = "../benches/contention_crucible/aerostore.rs"]
mod aerostore;
#[path = "../benches/contention_crucible/calibrated.rs"]
mod calibrated;
#[path = "../benches/contention_crucible/fixture.rs"]
mod fixture;
#[path = "../benches/contention_crucible/maintenance.rs"]
mod maintenance;
#[path = "../benches/contention_crucible/measurement.rs"]
mod measurement;
#[path = "../benches/contention_crucible/model.rs"]
mod model;
#[path = "../benches/contention_crucible/oracle.rs"]
mod oracle;
#[path = "../benches/contention_crucible/postgres.rs"]
mod postgres;
#[path = "../benches/contention_crucible/service.rs"]
mod service;
#[path = "../benches/contention_crucible/storage.rs"]
mod storage;
#[path = "../benches/contention_crucible/supervision.rs"]
mod supervision;
#[path = "../benches/contention_crucible/workers.rs"]
mod workers;
