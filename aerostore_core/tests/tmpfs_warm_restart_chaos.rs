#![cfg(target_os = "linux")]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aerostore_core::{
    alloc_u32_array, load_boot_layout, open_boot_context, persist_boot_layout, read_u32_array,
    BootLayout, BootMode, OccError, OccTable, OccTransaction, WalDeltaCodec,
};
use serde::{Deserialize, Serialize};
use tempfile::tempdir;

const CHILD_MODE_ENV: &str = "AEROSTORE_TMPFS_CHAOS_MODE";
const CHILD_SHM_PATH_ENV: &str = "AEROSTORE_TMPFS_CHAOS_SHM_PATH";
const CHILD_WAL_PATH_ENV: &str = "AEROSTORE_TMPFS_CHAOS_WAL_PATH";

const ROW_CAPACITY: usize = 65_536;
const TARGET_ROWS: usize = 50_000;
const SHM_BYTES: usize = 256 << 20;
const EXPECTED_WAL_BYTES: &[u8] = b"bad_wal_bytes_do_not_replay";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct WarmRow {
    exists: u8,
    value: u64,
}

impl WalDeltaCodec for WarmRow {}

impl WarmRow {
    #[inline]
    fn empty() -> Self {
        Self {
            exists: 0,
            value: 0,
        }
    }
}

fn maybe_run_child(mode: &str, run: fn(&Path, &Path)) {
    let observed_mode = std::env::var(CHILD_MODE_ENV).unwrap_or_default();
    if observed_mode != mode {
        return;
    }
    let shm_path = std::env::var(CHILD_SHM_PATH_ENV).expect("missing shm path env");
    let wal_path = std::env::var(CHILD_WAL_PATH_ENV).expect("missing wal path env");
    run(Path::new(&shm_path), Path::new(&wal_path));
    std::process::exit(0);
}

fn spawn_child(
    test_name: &str,
    mode: &str,
    shm_path: &Path,
    wal_path: &Path,
) -> std::process::ExitStatus {
    let exe = std::env::current_exe().expect("failed to resolve current test binary");
    Command::new(exe)
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .env(CHILD_MODE_ENV, mode)
        .env(CHILD_SHM_PATH_ENV, shm_path)
        .env(CHILD_WAL_PATH_ENV, wal_path)
        .status()
        .expect("failed to spawn child mode")
}

fn writer_child(shm_path: &Path, wal_path: &Path) {
    let boot =
        open_boot_context(Some(shm_path), SHM_BYTES).expect("writer failed to open boot context");
    assert_eq!(
        boot.mode,
        BootMode::ColdReplay,
        "writer expected cold replay mode for first boot"
    );
    let shm = Arc::clone(&boot.shm);
    let table = Arc::new(
        OccTable::<WarmRow>::new(Arc::clone(&shm), ROW_CAPACITY)
            .expect("writer failed to create OCC table"),
    );

    for row_id in 0..ROW_CAPACITY {
        table
            .seed_row(row_id, WarmRow::empty())
            .unwrap_or_else(|err| panic!("writer failed to seed row {row_id}: {err}"));
    }

    for row_id in 0..TARGET_ROWS {
        let next = WarmRow {
            exists: 1,
            value: row_id as u64,
        };
        write_one(&table, row_id, next)
            .unwrap_or_else(|err| panic!("writer failed to write row {row_id}: {err}"));
    }

    let mut layout = BootLayout::new(ROW_CAPACITY).expect("writer failed to allocate boot layout");
    layout.occ_shared_header_offset = table.shared_header_offset();
    let slot_offsets = table.index_slot_offsets();
    let (slot_offsets_offset, slot_offsets_len) =
        alloc_u32_array(shm.as_ref(), slot_offsets.as_slice())
            .expect("writer failed to persist table slot offsets");
    layout.occ_slot_offsets_offset = slot_offsets_offset;
    layout.occ_slot_offsets_len = slot_offsets_len;
    persist_boot_layout(shm.as_ref(), &layout).expect("writer failed to persist boot layout");

    std::fs::write(wal_path, EXPECTED_WAL_BYTES)
        .expect("writer failed to create wal sentinel bytes");
    std::process::exit(9);
}

fn reader_child(shm_path: &Path, wal_path: &Path) {
    let started = Instant::now();
    let boot =
        open_boot_context(Some(shm_path), SHM_BYTES).expect("reader failed to open boot context");
    assert_eq!(
        boot.mode,
        BootMode::WarmAttach,
        "reader expected warm attach mode"
    );

    let layout = load_boot_layout(boot.shm.as_ref())
        .expect("reader failed to decode boot layout")
        .expect("reader expected boot layout");
    let slot_offsets = read_u32_array(
        boot.shm.as_ref(),
        layout.occ_slot_offsets_offset,
        layout.occ_slot_offsets_len,
    )
    .expect("reader failed to load table slot offsets");
    let table = OccTable::<WarmRow>::from_existing(
        Arc::clone(&boot.shm),
        layout.occ_shared_header_offset,
        slot_offsets,
    )
    .expect("reader failed to attach OCC table from warm layout");

    let boot_elapsed = started.elapsed();
    assert!(
        boot_elapsed < Duration::from_millis(5),
        "warm boot took {:?}, expected < 5ms",
        boot_elapsed
    );

    for row_id in 0..TARGET_ROWS {
        let row = table
            .latest_value(row_id)
            .unwrap_or_else(|err| panic!("reader latest_value failed row={row_id}: {err}"))
            .unwrap_or_else(|| panic!("reader row {} unexpectedly missing", row_id));
        assert_eq!(row.exists, 1, "reader row {} exists flag mismatch", row_id);
        assert_eq!(
            row.value, row_id as u64,
            "reader row {} value mismatch after warm attach",
            row_id
        );
    }

    let observed_wal_bytes =
        std::fs::read(wal_path).expect("reader failed to read wal sentinel bytes");
    assert_eq!(
        observed_wal_bytes, EXPECTED_WAL_BYTES,
        "warm attach unexpectedly modified WAL bytes"
    );
}

fn write_one(table: &OccTable<WarmRow>, row_id: usize, value: WarmRow) -> Result<(), OccError> {
    loop {
        let mut tx: OccTransaction<WarmRow> = table.begin_transaction()?;
        table.write(&mut tx, row_id, value)?;
        match table.commit(&mut tx) {
            Ok(_) => return Ok(()),
            Err(OccError::SerializationFailure) => continue,
            Err(err) => return Err(err),
        }
    }
}

#[test]
fn warm_restart_chaos_boots_under_5ms_and_skips_wal_replay() {
    maybe_run_child("writer", writer_child as fn(&Path, &Path));
    maybe_run_child("reader", reader_child as fn(&Path, &Path));

    let temp = tempdir().expect("failed to create tempdir for chaos test");
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock moved backwards")
        .as_nanos();
    let shm_path = PathBuf::from(format!("/dev/shm/aerostore.mmap.{}", nonce));
    let wal_path = temp.path().join("aerostore.wal");

    let _ = std::fs::remove_file(&shm_path);
    let _ = std::fs::remove_file(&wal_path);

    let writer_status = spawn_child(
        "warm_restart_chaos_boots_under_5ms_and_skips_wal_replay",
        "writer",
        shm_path.as_path(),
        wal_path.as_path(),
    );
    assert_eq!(
        writer_status.code(),
        Some(9),
        "writer child should hard-exit with code 9, got status {}",
        writer_status
    );

    let reader_status = spawn_child(
        "warm_restart_chaos_boots_under_5ms_and_skips_wal_replay",
        "reader",
        shm_path.as_path(),
        wal_path.as_path(),
    );
    assert!(
        reader_status.success(),
        "reader child failed after warm restart: {}",
        reader_status
    );

    let _ = std::fs::remove_file(shm_path);
    let _ = std::fs::remove_file(wal_path);
}
