#![cfg(target_os = "linux")]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aerostore_core::{
    alloc_u32_array, load_boot_layout, open_boot_context, persist_boot_layout, read_u32_array,
    recover_occ_table_from_wal, BootLayout, BootMode, OccError, OccTable, ShmArena,
};

const ROW_CAPACITY: usize = 50_000;
const SHM_BYTES: usize = 256 << 20;
const WARM_BOOT_BUDGET: Duration = Duration::from_millis(5);
const WARM_COLD_RATIO_MAX: f64 = 0.20;
const RESTART_CYCLES: usize = 20;
const THROUGHPUT_UPDATES: usize = 12_000;
const POST_RESTART_TPS_FLOOR_RATIO: f64 = 0.70;
const MALFORMED_WAL_BYTES: &[u8] = b"malformed_wal_bytes_for_warm_attach_guard";

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

#[derive(Clone, Debug)]
struct BaselineState {
    cold_elapsed: Duration,
    layout: BootLayout,
    slot_offsets: Vec<u32>,
    head_offset: u32,
    checksum: u128,
}

#[derive(Clone, Debug)]
struct WarmAttachState {
    warm_elapsed: Duration,
    layout: BootLayout,
    slot_offsets: Vec<u32>,
    head_offset: u32,
    checksum: u128,
    orphaned_proc_slots_cleared: usize,
}

#[test]
fn warm_vs_cold_boot_latency_is_constant_time_for_warm_attach() {
    let shm_path = unique_tmpfs_path("aerostore_warm_cold_latency");
    let _ = std::fs::remove_file(&shm_path);

    let baseline = build_cold_fixture(shm_path.as_path());
    let warm = capture_warm_attach_state(shm_path.as_path());

    assert!(
        warm.warm_elapsed < WARM_BOOT_BUDGET,
        "warm attach took {:?}, expected < {:?}",
        warm.warm_elapsed,
        WARM_BOOT_BUDGET
    );

    let ratio = warm.warm_elapsed.as_secs_f64() / baseline.cold_elapsed.as_secs_f64().max(1e-9);
    assert!(
        ratio <= WARM_COLD_RATIO_MAX,
        "warm/cold startup ratio too high: warm={:?} cold={:?} ratio={:.4} max={:.4}",
        warm.warm_elapsed,
        baseline.cold_elapsed,
        ratio,
        WARM_COLD_RATIO_MAX
    );
    assert_eq!(
        warm.checksum, baseline.checksum,
        "warm attach checksum mismatch after cold fixture initialization"
    );

    let _ = std::fs::remove_file(shm_path);
}

#[test]
fn warm_restart_cycles_preserve_state_without_allocator_growth() {
    let shm_path = unique_tmpfs_path("aerostore_warm_restart_cycles");
    let _ = std::fs::remove_file(&shm_path);

    let baseline = build_cold_fixture(shm_path.as_path());
    let mut worst_boot = Duration::ZERO;
    for cycle in 0..RESTART_CYCLES {
        let warm = capture_warm_attach_state(shm_path.as_path());
        worst_boot = worst_boot.max(warm.warm_elapsed);

        assert!(
            warm.warm_elapsed < WARM_BOOT_BUDGET,
            "cycle {} warm attach took {:?}, expected < {:?}",
            cycle,
            warm.warm_elapsed,
            WARM_BOOT_BUDGET
        );
        assert_eq!(
            warm.orphaned_proc_slots_cleared, 0,
            "cycle {} unexpectedly cleared orphaned procarray slots",
            cycle
        );
        assert_eq!(
            warm.layout.occ_shared_header_offset, baseline.layout.occ_shared_header_offset,
            "cycle {} shared header offset drifted",
            cycle
        );
        assert_eq!(
            warm.layout.occ_slot_offsets_offset, baseline.layout.occ_slot_offsets_offset,
            "cycle {} slot offsets pointer drifted",
            cycle
        );
        assert_eq!(
            warm.layout.occ_slot_offsets_len, baseline.layout.occ_slot_offsets_len,
            "cycle {} slot offsets length drifted",
            cycle
        );
        assert_eq!(
            warm.slot_offsets, baseline.slot_offsets,
            "cycle {} slot offsets content drifted",
            cycle
        );
        assert_eq!(
            warm.head_offset, baseline.head_offset,
            "cycle {} observed shared allocator head growth (possible warm-restart leak)",
            cycle
        );
        assert_eq!(
            warm.checksum, baseline.checksum,
            "cycle {} warm attach checksum drifted",
            cycle
        );
    }

    eprintln!(
        "warm_restart_cycle_stability: cycles={} worst_boot={:?} baseline_head_offset={}",
        RESTART_CYCLES, worst_boot, baseline.head_offset
    );

    let _ = std::fs::remove_file(shm_path);
}

#[test]
fn immediate_post_restart_throughput_stays_within_target_ratio() {
    let shm_path = unique_tmpfs_path("aerostore_post_restart_tps");
    let _ = std::fs::remove_file(&shm_path);
    let _ = build_cold_fixture(shm_path.as_path());

    let (first_shm, first_table, first_boot_elapsed) = warm_attach_table(shm_path.as_path());
    assert!(
        first_boot_elapsed < WARM_BOOT_BUDGET,
        "first warm attach took {:?}, expected < {:?}",
        first_boot_elapsed,
        WARM_BOOT_BUDGET
    );
    let baseline_tps = measure_update_throughput(&first_table, THROUGHPUT_UPDATES);
    drop(first_table);
    drop(first_shm);

    let (second_shm, second_table, second_boot_elapsed) = warm_attach_table(shm_path.as_path());
    assert!(
        second_boot_elapsed < WARM_BOOT_BUDGET,
        "second warm attach took {:?}, expected < {:?}",
        second_boot_elapsed,
        WARM_BOOT_BUDGET
    );
    let immediate_tps = measure_update_throughput(&second_table, THROUGHPUT_UPDATES);
    drop(second_table);
    drop(second_shm);

    let ratio = immediate_tps / baseline_tps.max(f64::EPSILON);
    eprintln!(
        "warm_restart_post_boot_throughput: baseline_tps={:.2} immediate_tps={:.2} ratio={:.3}",
        baseline_tps, immediate_tps, ratio
    );
    assert!(
        ratio >= POST_RESTART_TPS_FLOOR_RATIO,
        "immediate post-restart throughput dropped too far: baseline={:.2} immediate={:.2} ratio={:.3} floor={:.3}",
        baseline_tps,
        immediate_tps,
        ratio,
        POST_RESTART_TPS_FLOOR_RATIO
    );

    let _ = std::fs::remove_file(shm_path);
}

#[test]
fn warm_attach_does_not_touch_wal_and_cold_control_fails_on_malformed_wal() {
    let shm_path = unique_tmpfs_path("aerostore_wal_touch_guard");
    let _ = std::fs::remove_file(&shm_path);
    let _ = build_cold_fixture(shm_path.as_path());

    let wal_path = std::env::temp_dir().join(format!("aerostore_wal_touch_{}.wal", unique_nonce()));
    std::fs::write(&wal_path, MALFORMED_WAL_BYTES).expect("failed to write malformed wal bytes");
    let wal_before = wal_signature(wal_path.as_path());

    let (_warm_shm, warm_table, warm_elapsed) = warm_attach_table(shm_path.as_path());
    assert!(
        warm_elapsed < WARM_BOOT_BUDGET,
        "warm attach took {:?}, expected < {:?}",
        warm_elapsed,
        WARM_BOOT_BUDGET
    );
    let sample = warm_table
        .latest_value(12_345)
        .expect("failed to read sample row on warm attach")
        .expect("sample row missing after warm attach");
    assert_eq!(sample, WarmRow::seeded(12_345));
    drop(warm_table);

    let wal_after = wal_signature(wal_path.as_path());
    assert_eq!(
        wal_before.bytes, wal_after.bytes,
        "warm attach mutated WAL bytes unexpectedly"
    );
    assert_eq!(
        wal_before.len, wal_after.len,
        "warm attach changed WAL file size unexpectedly"
    );
    assert_eq!(
        wal_before.modified_epoch, wal_after.modified_epoch,
        "warm attach changed WAL modified timestamp unexpectedly"
    );

    let _ = std::fs::remove_file(&shm_path);
    let control_shm = Arc::new(ShmArena::new(8 << 20).expect("failed to create control shm"));
    let control_table =
        OccTable::<u64>::new(Arc::clone(&control_shm), 1).expect("failed to create control table");
    control_table
        .seed_row(0, 0_u64)
        .expect("failed to seed control row");

    let cold_replay = recover_occ_table_from_wal(&control_table, wal_path.as_path());
    assert!(
        cold_replay.is_err(),
        "cold replay control unexpectedly accepted malformed wal bytes"
    );

    let _ = std::fs::remove_file(shm_path);
    let _ = std::fs::remove_file(wal_path);
}

fn build_cold_fixture(shm_path: &Path) -> BaselineState {
    let started = Instant::now();
    let boot = open_boot_context(Some(shm_path), SHM_BYTES).expect("failed to open cold context");
    assert_eq!(
        boot.mode,
        BootMode::ColdReplay,
        "cold fixture expected BootMode::ColdReplay"
    );

    let shm = Arc::clone(&boot.shm);
    let table = OccTable::<WarmRow>::new(Arc::clone(&shm), ROW_CAPACITY)
        .expect("failed to create cold fixture table");
    for row_id in 0..ROW_CAPACITY {
        table
            .seed_row(row_id, WarmRow::seeded(row_id))
            .unwrap_or_else(|err| panic!("failed to seed cold fixture row {row_id}: {err}"));
    }

    let mut layout = BootLayout::new(ROW_CAPACITY).expect("failed to build layout metadata");
    layout.occ_shared_header_offset = table.shared_header_offset();
    let slot_offsets = table.index_slot_offsets();
    let (slot_offsets_offset, slot_offsets_len) =
        alloc_u32_array(shm.as_ref(), slot_offsets.as_slice())
            .expect("failed to persist slot offsets");
    layout.occ_slot_offsets_offset = slot_offsets_offset;
    layout.occ_slot_offsets_len = slot_offsets_len;
    persist_boot_layout(shm.as_ref(), &layout).expect("failed to persist cold fixture layout");

    let persisted_layout = load_boot_layout(shm.as_ref())
        .expect("failed to decode persisted layout")
        .expect("persisted layout missing");
    let persisted_slot_offsets = read_u32_array(
        shm.as_ref(),
        persisted_layout.occ_slot_offsets_offset,
        persisted_layout.occ_slot_offsets_len,
    )
    .expect("failed to decode persisted slot offsets");
    let checksum = table_checksum(&table);
    let head_offset = shm.chunked_arena().head_offset();
    let cold_elapsed = started.elapsed();

    BaselineState {
        cold_elapsed,
        layout: persisted_layout,
        slot_offsets: persisted_slot_offsets,
        head_offset,
        checksum,
    }
}

fn capture_warm_attach_state(shm_path: &Path) -> WarmAttachState {
    let started = Instant::now();
    let boot = open_boot_context(Some(shm_path), SHM_BYTES).expect("failed to open warm context");
    assert_eq!(
        boot.mode,
        BootMode::WarmAttach,
        "warm attach expected BootMode::WarmAttach"
    );
    let shm = Arc::clone(&boot.shm);
    let layout = load_boot_layout(shm.as_ref())
        .expect("failed to load warm layout")
        .expect("missing warm layout");
    let slot_offsets = read_u32_array(
        shm.as_ref(),
        layout.occ_slot_offsets_offset,
        layout.occ_slot_offsets_len,
    )
    .expect("failed to read warm slot offsets");
    let table = OccTable::<WarmRow>::from_existing(
        Arc::clone(&shm),
        layout.occ_shared_header_offset,
        slot_offsets.clone(),
    )
    .expect("failed to attach warm table");
    let warm_elapsed = started.elapsed();
    let checksum = table_checksum(&table);
    let head_offset = shm.chunked_arena().head_offset();

    WarmAttachState {
        warm_elapsed,
        layout,
        slot_offsets,
        head_offset,
        checksum,
        orphaned_proc_slots_cleared: boot.orphaned_proc_slots_cleared,
    }
}

fn warm_attach_table(shm_path: &Path) -> (Arc<ShmArena>, OccTable<WarmRow>, Duration) {
    let started = Instant::now();
    let boot = open_boot_context(Some(shm_path), SHM_BYTES).expect("failed to open warm context");
    assert_eq!(
        boot.mode,
        BootMode::WarmAttach,
        "warm attach expected BootMode::WarmAttach"
    );
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
    let elapsed = started.elapsed();
    (shm, table, elapsed)
}

fn table_checksum(table: &OccTable<WarmRow>) -> u128 {
    let mut checksum = 0_u128;
    for row_id in 0..ROW_CAPACITY {
        let row = table
            .latest_value(row_id)
            .unwrap_or_else(|err| panic!("latest_value failed row={row_id}: {err}"))
            .unwrap_or_else(|| panic!("missing seeded row {row_id}"));
        checksum = checksum
            .wrapping_mul(1_099_511_628_211_u128)
            .wrapping_add((row.value as u128) ^ ((row_id as u128) << 1) ^ (row.exists as u128));
    }
    checksum
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct WalSignature {
    bytes: Vec<u8>,
    len: u64,
    modified_epoch: Option<Duration>,
}

fn wal_signature(path: &Path) -> WalSignature {
    let bytes = std::fs::read(path).expect("failed to read wal bytes");
    let metadata = std::fs::metadata(path).expect("failed to read wal metadata");
    let modified_epoch = metadata
        .modified()
        .ok()
        .and_then(|ts| ts.duration_since(UNIX_EPOCH).ok());
    WalSignature {
        bytes,
        len: metadata.len(),
        modified_epoch,
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
