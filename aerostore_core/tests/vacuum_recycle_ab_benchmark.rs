use std::sync::Arc;
use std::time::Instant;

use aerostore_core::{
    spawn_vacuum_daemon_with_config, OccError, OccRow, OccTable, ShmArena, VacuumDaemonConfig,
};

const ARENA_BYTES: usize = 10 << 20;
const TOTAL_UPDATES: usize = 500_000;
const VACUUM_TPS_FLOOR: f64 = 50_000.0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CounterRow {
    value: u64,
}

#[derive(Debug)]
struct RunStats {
    completed: usize,
    tps: f64,
    oom: bool,
    head_growth: u64,
}

#[test]
fn benchmark_vacuum_enabled_outlasts_disabled_under_hot_row_stress() {
    let with_vacuum = run_workload(true);
    let without_vacuum = run_workload(false);

    assert!(
        with_vacuum.completed == TOTAL_UPDATES && !with_vacuum.oom,
        "vacuum-enabled run must complete all updates (stats={:?})",
        with_vacuum
    );
    assert!(
        with_vacuum.tps >= VACUUM_TPS_FLOOR,
        "vacuum-enabled throughput below strict floor: {:.2} < {:.2} updates/sec",
        with_vacuum.tps,
        VACUUM_TPS_FLOOR
    );

    assert!(
        without_vacuum.oom,
        "vacuum-disabled run should OOM under 10MB stress (stats={:?})",
        without_vacuum
    );
    assert!(
        without_vacuum.completed < TOTAL_UPDATES,
        "vacuum-disabled run unexpectedly completed all updates (stats={:?})",
        without_vacuum
    );
    assert!(
        without_vacuum.completed <= (TOTAL_UPDATES * 6) / 10,
        "vacuum-disabled run progressed too far before OOM; expected <=60%, got {}",
        without_vacuum.completed
    );
    assert!(
        with_vacuum.completed >= without_vacuum.completed.saturating_mul(2) - 1,
        "vacuum-enabled completed-updates advantage is too small: with={} without={}",
        with_vacuum.completed,
        without_vacuum.completed
    );
    assert!(
        with_vacuum.head_growth.saturating_mul(3) < without_vacuum.head_growth.saturating_mul(2),
        "vacuum-enabled head growth should be materially lower than disabled run (with={} without={})",
        with_vacuum.head_growth,
        without_vacuum.head_growth
    );
}

fn run_workload(enable_vacuum: bool) -> RunStats {
    let shm = Arc::new(ShmArena::new(ARENA_BYTES).expect("failed to create shared arena"));
    let table = Arc::new(OccTable::<CounterRow>::new(Arc::clone(&shm), 1).expect("create table"));
    table
        .seed_row(0, CounterRow { value: 0 })
        .expect("seed row failed");
    let base_head = shm.chunked_arena().head_offset();

    let _vacuum = if enable_vacuum {
        Some(
            spawn_vacuum_daemon_with_config(
                Arc::clone(&table),
                VacuumDaemonConfig::default().with_interval(std::time::Duration::from_millis(5)),
            )
            .expect("spawn vacuum daemon"),
        )
    } else {
        None
    };

    let started = Instant::now();
    let mut completed = 0_usize;
    let mut hit_oom = false;
    for _ in 0..TOTAL_UPDATES {
        match apply_one_increment(table.as_ref()) {
            Ok(()) => completed += 1,
            Err(WorkloadError::OutOfMemory) => {
                hit_oom = true;
                break;
            }
        }
    }
    let elapsed_secs = started.elapsed().as_secs_f64().max(f64::EPSILON);
    let tps = completed as f64 / elapsed_secs;
    let head_growth = shm.chunked_arena().head_offset().saturating_sub(base_head) as u64;

    RunStats {
        completed,
        tps,
        oom: hit_oom,
        head_growth,
    }
}

enum WorkloadError {
    OutOfMemory,
}

fn apply_one_increment(table: &OccTable<CounterRow>) -> Result<(), WorkloadError> {
    loop {
        let mut tx = match table.begin_transaction() {
            Ok(tx) => tx,
            Err(err) => {
                if is_oom_error(&err) {
                    return Err(WorkloadError::OutOfMemory);
                }
                panic!("begin_transaction failed: {}", err);
            }
        };

        let current = match table.read(&mut tx, 0) {
            Ok(Some(value)) => value,
            Ok(None) => panic!("row missing"),
            Err(OccError::SerializationFailure) => {
                let _ = table.abort(&mut tx);
                continue;
            }
            Err(err) => {
                if is_oom_error(&err) {
                    return Err(WorkloadError::OutOfMemory);
                }
                panic!("read failed: {}", err);
            }
        };

        match table.write(
            &mut tx,
            0,
            CounterRow {
                value: current.value + 1,
            },
        ) {
            Ok(()) => {}
            Err(OccError::SerializationFailure) => {
                let _ = table.abort(&mut tx);
                continue;
            }
            Err(err) => {
                if is_oom_error(&err) {
                    let _ = table.abort(&mut tx);
                    return Err(WorkloadError::OutOfMemory);
                }
                panic!("write failed: {}", err);
            }
        }

        match table.commit(&mut tx) {
            Ok(1) => return Ok(()),
            Ok(other) => panic!("unexpected committed write count: {}", other),
            Err(OccError::SerializationFailure) => continue,
            Err(err) => {
                if is_oom_error(&err) {
                    return Err(WorkloadError::OutOfMemory);
                }
                panic!("commit failed: {}", err);
            }
        }
    }
}

fn is_oom_error(err: &OccError) -> bool {
    if let OccError::Allocation(message) = err {
        message.contains("out of memory")
    } else {
        false
    }
}

#[test]
fn sanity_occ_row_size_expectation_for_10mb_stress_math() {
    let row_size = std::mem::size_of::<OccRow<CounterRow>>();
    assert!(
        row_size >= 24 && row_size <= 256,
        "unexpected OccRow<CounterRow> size {}; revisit A/B stress thresholds",
        row_size
    );
}
