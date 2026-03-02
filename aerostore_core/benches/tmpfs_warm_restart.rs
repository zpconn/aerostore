#![cfg(target_os = "linux")]

use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aerostore_core::{
    alloc_u32_array, load_boot_layout, open_boot_context, persist_boot_layout, read_u32_array,
    BootLayout, BootMode, OccError, OccTable, ShmArena,
};
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};

const ROW_CAPACITY: usize = 50_000;
const SHM_BYTES: usize = 256 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct WarmRow {
    exists: u8,
    value: u64,
}

impl WarmRow {
    #[inline]
    fn seeded(row_id: usize) -> Self {
        Self {
            exists: 1,
            value: row_id as u64,
        }
    }
}

fn bench_tmpfs_warm_restart(c: &mut Criterion) {
    let mut group = c.benchmark_group("tmpfs_warm_restart");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));

    group.bench_function("warm_attach_boot", |b| {
        let shm_path = unique_tmpfs_path("aerostore_bench_warm_attach");
        let _ = std::fs::remove_file(&shm_path);
        let _ = build_cold_fixture(shm_path.as_path());
        b.iter(|| black_box(warm_attach_elapsed(shm_path.as_path())));
        let _ = std::fs::remove_file(shm_path);
    });

    group.bench_function("cold_boot_fixture_build", |b| {
        b.iter_batched(
            || {
                let path = unique_tmpfs_path("aerostore_bench_cold_boot");
                let _ = std::fs::remove_file(&path);
                path
            },
            |path| {
                let elapsed = build_cold_fixture(path.as_path());
                let _ = std::fs::remove_file(path);
                black_box(elapsed)
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("post_restart_update_throughput", |b| {
        b.iter_batched(
            || {
                let shm_path = unique_tmpfs_path("aerostore_bench_post_restart");
                let _ = std::fs::remove_file(&shm_path);
                let _ = build_cold_fixture(shm_path.as_path());
                shm_path
            },
            |shm_path| {
                let (shm, table) = warm_attach_table(shm_path.as_path());
                let tps = measure_update_throughput(&table, 4_000);
                drop(table);
                drop(shm);
                let _ = std::fs::remove_file(shm_path);
                black_box(tps)
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn build_cold_fixture(shm_path: &Path) -> Duration {
    let started = Instant::now();
    let boot = open_boot_context(Some(shm_path), SHM_BYTES).expect("failed to open cold context");
    assert_eq!(boot.mode, BootMode::ColdReplay);

    let shm = Arc::clone(&boot.shm);
    let table = OccTable::<WarmRow>::new(Arc::clone(&shm), ROW_CAPACITY)
        .expect("failed to create fixture table");
    for row_id in 0..ROW_CAPACITY {
        table
            .seed_row(row_id, WarmRow::seeded(row_id))
            .unwrap_or_else(|err| panic!("failed to seed row {row_id}: {err}"));
    }

    let mut layout = BootLayout::new(ROW_CAPACITY).expect("failed to create warm layout");
    layout.occ_shared_header_offset = table.shared_header_offset();
    let slot_offsets = table.index_slot_offsets();
    let (slot_offsets_offset, slot_offsets_len) =
        alloc_u32_array(shm.as_ref(), slot_offsets.as_slice())
            .expect("failed to persist slot offsets");
    layout.occ_slot_offsets_offset = slot_offsets_offset;
    layout.occ_slot_offsets_len = slot_offsets_len;
    persist_boot_layout(shm.as_ref(), &layout).expect("failed to persist warm layout");

    started.elapsed()
}

fn warm_attach_elapsed(shm_path: &Path) -> Duration {
    let started = Instant::now();
    let (_shm, _table) = warm_attach_table(shm_path);
    started.elapsed()
}

fn warm_attach_table(shm_path: &Path) -> (Arc<ShmArena>, OccTable<WarmRow>) {
    let boot = open_boot_context(Some(shm_path), SHM_BYTES).expect("failed to open warm context");
    assert_eq!(boot.mode, BootMode::WarmAttach);
    let shm = Arc::clone(&boot.shm);
    let layout = load_boot_layout(shm.as_ref())
        .expect("failed to decode warm layout")
        .expect("missing warm layout");
    let slot_offsets = read_u32_array(
        shm.as_ref(),
        layout.occ_slot_offsets_offset,
        layout.occ_slot_offsets_len,
    )
    .expect("failed to decode warm slot offsets");
    let table = OccTable::<WarmRow>::from_existing(
        Arc::clone(&shm),
        layout.occ_shared_header_offset,
        slot_offsets,
    )
    .expect("failed to attach warm table");
    (shm, table)
}

fn measure_update_throughput(table: &OccTable<WarmRow>, updates: usize) -> f64 {
    let started = Instant::now();
    for step in 0..updates {
        let row_id = step % ROW_CAPACITY;
        let next = WarmRow {
            exists: 1,
            value: ((step as u64) << 32) ^ (row_id as u64),
        };
        write_one(table, row_id, next);
    }
    let elapsed = started.elapsed();
    updates as f64 / elapsed.as_secs_f64().max(f64::EPSILON)
}

fn write_one(table: &OccTable<WarmRow>, row_id: usize, value: WarmRow) {
    loop {
        let mut tx = table
            .begin_transaction()
            .unwrap_or_else(|err| panic!("begin_transaction failed row={row_id}: {err}"));
        table
            .write(&mut tx, row_id, value)
            .unwrap_or_else(|err| panic!("write failed row={row_id}: {err}"));
        match table.commit(&mut tx) {
            Ok(_) => return,
            Err(OccError::SerializationFailure) => continue,
            Err(err) => panic!("commit failed row={row_id}: {err}"),
        }
    }
}

fn unique_nonce() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock moved backwards")
        .as_nanos()
}

fn unique_tmpfs_path(prefix: &str) -> PathBuf {
    PathBuf::from(format!("/dev/shm/{prefix}_{}.mmap", unique_nonce()))
}

criterion_group!(benches, bench_tmpfs_warm_restart);
criterion_main!(benches);
