use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StoreMetrics {
    pub begins: u64,
    pub commits: u64,
    pub aborts: u64,
    pub reads: u64,
    pub queries: u64,
    pub returned_rows: u64,
    pub writes: u64,
    pub savepoints: u64,
    pub savepoint_rollbacks: u64,
    pub index_mutations: u64,
}

impl StoreMetrics {
    pub fn add(&mut self, other: &Self) {
        self.begins += other.begins;
        self.commits += other.commits;
        self.aborts += other.aborts;
        self.reads += other.reads;
        self.queries += other.queries;
        self.returned_rows += other.returned_rows;
        self.writes += other.writes;
        self.savepoints += other.savepoints;
        self.savepoint_rollbacks += other.savepoint_rollbacks;
        self.index_mutations += other.index_mutations;
    }
}
