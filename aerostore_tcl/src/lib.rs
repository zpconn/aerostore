use std::collections::{HashMap, HashSet};
use std::os::raw::{c_char, c_int};
use std::path::{Path, PathBuf};
use std::slice;
use std::str;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use aerostore_core::{
    alloc_u32_array, clear_persisted_boot_layout, load_boot_layout, open_boot_context,
    persist_boot_layout as persist_shared_boot_layout, read_u32_array,
    recover_occ_table_from_checkpoint_and_wal_with_pk_map, spawn_wal_writer_daemon,
    write_occ_checkpoint_and_truncate_wal, BootLayout, BootMode, IndexValue, IngestStats, OccError,
    OccTable, OccTransaction, PlannerError, RelPtr, RetryBackoff, RetryPolicy, RuleBasedOptimizer,
    SchemaCatalog, SecondaryIndex, SharedWalRing, ShmArena, ShmPrimaryKeyMap, StapiRow, StapiValue,
    SynchronousCommit, TsvColumns, TsvDecodeError, WalDeltaCodec, WalDeltaError, WalRing,
    WalWriterError, BOOT_LAYOUT_MAX_INDEXES, DEFAULT_TMPFS_PATH, SYNCHRONOUS_COMMIT_KEY,
};
use serde::{Deserialize, Serialize};
use tcl::Interp;

const PACKAGE_NAME: &str = "aerostore";
const PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");
const DEFAULT_DATA_DIR: &str = "./aerostore_tcl_data";
const DEFAULT_INGEST_BATCH_SIZE: usize = 1024;
const DEFAULT_ROW_CAPACITY: usize = 1 << 15;
const DEFAULT_SHM_ARENA_BYTES: usize = 256 << 20;
const FLIGHT_ID_BYTES: usize = 24;
const WAL_RING_SLOTS: usize = 512;
const WAL_RING_SLOT_BYTES: usize = 256;

const WAL_FILE_NAME: &str = "aerostore.wal";
const CHECKPOINT_FILE_NAME: &str = "occ_checkpoint.dat";
const CHECKPOINT_INTERVAL_SECS_KEY: &str = "aerostore.checkpoint_interval_secs";
const DEFAULT_CHECKPOINT_INTERVAL_SECS: u64 = 300;
const SHM_PATH_ENV_KEY: &str = "AEROSTORE_SHM_PATH";
const MAX_BATCH_RETRY_ATTEMPTS: u32 = RetryPolicy::MAX_RETRIES_PER_UNIT;

const INDEX_SLOT_FLIGHT_ID: usize = 0;
const INDEX_SLOT_ALTITUDE: usize = 1;
const INDEX_SLOT_GS: usize = 2;
const INDEX_SLOT_LAT: usize = 3;
const INDEX_SLOT_LON: usize = 4;
const INDEX_SLOT_UPDATED_AT: usize = 5;
const INDEX_SLOT_COUNT: usize = 6;

type FlightDb = SharedFlightDb;

static GLOBAL_DB: OnceLock<Arc<FlightDb>> = OnceLock::new();
static GLOBAL_DB_DIR: OnceLock<String> = OnceLock::new();

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
struct FlightState {
    exists: u8,
    flight_id: [u8; FLIGHT_ID_BYTES],
    lat_scaled: i64,
    lon_scaled: i64,
    altitude: i32,
    gs: u16,
    updated_at: u64,
}

impl FlightState {
    const MASK_EXISTS: u64 = 1_u64 << 0;
    const MASK_FLIGHT_ID: u64 = 1_u64 << 1;
    const MASK_LAT: u64 = 1_u64 << 2;
    const MASK_LON: u64 = 1_u64 << 3;
    const MASK_ALTITUDE: u64 = 1_u64 << 4;
    const MASK_GS: u64 = 1_u64 << 5;
    const MASK_UPDATED_AT: u64 = 1_u64 << 6;

    #[inline]
    fn empty() -> Self {
        Self {
            exists: 0,
            flight_id: [0_u8; FLIGHT_ID_BYTES],
            lat_scaled: 0,
            lon_scaled: 0,
            altitude: 0,
            gs: 0,
            updated_at: 0,
        }
    }

    fn from_decoded(
        flight_id: &str,
        lat: f64,
        lon: f64,
        altitude: i32,
        gs: u16,
        updated_at: u64,
    ) -> Result<Self, String> {
        let encoded_id = encode_fixed_ascii::<FLIGHT_ID_BYTES>(flight_id).ok_or_else(|| {
            format!(
                "flight_id '{}' exceeds {} bytes",
                flight_id, FLIGHT_ID_BYTES
            )
        })?;

        Ok(Self {
            exists: 1,
            flight_id: encoded_id,
            lat_scaled: scale_coord(lat),
            lon_scaled: scale_coord(lon),
            altitude,
            gs,
            updated_at,
        })
    }

    #[inline]
    fn flight_id_string(&self) -> String {
        decode_fixed_ascii(self.flight_id.as_slice())
    }
}

impl WalDeltaCodec for FlightState {
    const COLUMN_COUNT: u8 = 7;

    fn wal_primary_key(_row_id_hint: usize, value: &Self) -> String {
        value.flight_id_string()
    }

    fn compute_dirty_mask(base_value: &Self, new_value: &Self) -> Result<u64, WalDeltaError> {
        let mut mask = 0_u64;
        if base_value.exists != new_value.exists {
            mask |= Self::MASK_EXISTS;
        }
        if base_value.flight_id != new_value.flight_id {
            mask |= Self::MASK_FLIGHT_ID;
        }
        if base_value.lat_scaled != new_value.lat_scaled {
            mask |= Self::MASK_LAT;
        }
        if base_value.lon_scaled != new_value.lon_scaled {
            mask |= Self::MASK_LON;
        }
        if base_value.altitude != new_value.altitude {
            mask |= Self::MASK_ALTITUDE;
        }
        if base_value.gs != new_value.gs {
            mask |= Self::MASK_GS;
        }
        if base_value.updated_at != new_value.updated_at {
            mask |= Self::MASK_UPDATED_AT;
        }
        Ok(mask)
    }

    fn encode_changed_fields(
        value: &Self,
        dirty_mask: u64,
        out: &mut Vec<u8>,
    ) -> Result<(), WalDeltaError> {
        if dirty_mask & Self::MASK_EXISTS != 0 {
            out.push(value.exists);
        }
        if dirty_mask & Self::MASK_FLIGHT_ID != 0 {
            out.extend_from_slice(value.flight_id.as_slice());
        }
        if dirty_mask & Self::MASK_LAT != 0 {
            out.extend_from_slice(value.lat_scaled.to_le_bytes().as_slice());
        }
        if dirty_mask & Self::MASK_LON != 0 {
            out.extend_from_slice(value.lon_scaled.to_le_bytes().as_slice());
        }
        if dirty_mask & Self::MASK_ALTITUDE != 0 {
            out.extend_from_slice(value.altitude.to_le_bytes().as_slice());
        }
        if dirty_mask & Self::MASK_GS != 0 {
            out.extend_from_slice(value.gs.to_le_bytes().as_slice());
        }
        if dirty_mask & Self::MASK_UPDATED_AT != 0 {
            out.extend_from_slice(value.updated_at.to_le_bytes().as_slice());
        }
        Ok(())
    }

    fn apply_changed_fields(
        base_value: &mut Self,
        dirty_mask: u64,
        delta_bytes: &[u8],
    ) -> Result<(), WalDeltaError> {
        let mut cursor = 0_usize;
        let mut take = |len: usize| -> Result<&[u8], WalDeltaError> {
            if cursor + len > delta_bytes.len() {
                return Err(WalDeltaError::InvalidDeltaLength {
                    expected_min: cursor + len,
                    expected_max: cursor + len,
                    actual: delta_bytes.len(),
                });
            }
            let slice = &delta_bytes[cursor..cursor + len];
            cursor += len;
            Ok(slice)
        };

        if dirty_mask & Self::MASK_EXISTS != 0 {
            base_value.exists = take(1)?[0];
        }
        if dirty_mask & Self::MASK_FLIGHT_ID != 0 {
            base_value.flight_id.copy_from_slice(take(FLIGHT_ID_BYTES)?);
        }
        if dirty_mask & Self::MASK_LAT != 0 {
            let mut arr = [0_u8; 8];
            arr.copy_from_slice(take(8)?);
            base_value.lat_scaled = i64::from_le_bytes(arr);
        }
        if dirty_mask & Self::MASK_LON != 0 {
            let mut arr = [0_u8; 8];
            arr.copy_from_slice(take(8)?);
            base_value.lon_scaled = i64::from_le_bytes(arr);
        }
        if dirty_mask & Self::MASK_ALTITUDE != 0 {
            let mut arr = [0_u8; 4];
            arr.copy_from_slice(take(4)?);
            base_value.altitude = i32::from_le_bytes(arr);
        }
        if dirty_mask & Self::MASK_GS != 0 {
            let mut arr = [0_u8; 2];
            arr.copy_from_slice(take(2)?);
            base_value.gs = u16::from_le_bytes(arr);
        }
        if dirty_mask & Self::MASK_UPDATED_AT != 0 {
            let mut arr = [0_u8; 8];
            arr.copy_from_slice(take(8)?);
            base_value.updated_at = u64::from_le_bytes(arr);
        }

        if cursor != delta_bytes.len() {
            return Err(WalDeltaError::InvalidDeltaLength {
                expected_min: cursor,
                expected_max: cursor,
                actual: delta_bytes.len(),
            });
        }
        Ok(())
    }
}

impl StapiRow for FlightState {
    fn has_field(field: &str) -> bool {
        matches!(
            field,
            "exists"
                | "flight_id"
                | "flight"
                | "ident"
                | "altitude"
                | "alt"
                | "lat"
                | "lon"
                | "long"
                | "longitude"
                | "gs"
                | "groundspeed"
                | "updated_at"
                | "updated"
                | "ts"
        )
    }

    fn field_value(&self, field: &str) -> Option<StapiValue> {
        match field {
            "exists" => Some(StapiValue::Int(self.exists as i64)),
            "flight_id" | "flight" | "ident" => Some(StapiValue::Text(self.flight_id_string())),
            "altitude" | "alt" => Some(StapiValue::Int(self.altitude as i64)),
            "lat" => Some(StapiValue::Int(self.lat_scaled)),
            "lon" | "long" | "longitude" => Some(StapiValue::Int(self.lon_scaled)),
            "gs" | "groundspeed" => Some(StapiValue::Int(self.gs as i64)),
            "updated_at" | "updated" | "ts" => {
                i64::try_from(self.updated_at).ok().map(StapiValue::Int)
            }
            _ => None,
        }
    }
}

struct SearchRequest {
    stapi: String,
    limit: Option<usize>,
    offset: usize,
    sort_desc: bool,
    has_sort: bool,
}

struct FlightTsvDecoder;

impl FlightTsvDecoder {
    fn decode(&self, cols: &mut TsvColumns<'_>) -> Result<(String, FlightState), TsvDecodeError> {
        let flight_id = cols.expect_str()?.to_owned();
        let lat = cols.expect_f64()?;
        let lon = cols.expect_f64()?;
        let altitude = cols.expect_i32()?;
        let gs = cols.expect_u16()?;
        let updated_at = cols.expect_u64()?;

        let row = FlightState::from_decoded(flight_id.as_str(), lat, lon, altitude, gs, updated_at)
            .map_err(|_| TsvDecodeError::new(1, "flight_id exceeds fixed-width capacity"))?;

        Ok((flight_id, row))
    }
}

struct PendingIndexUpdate {
    before: FlightState,
    after: FlightState,
}

#[derive(Clone)]
struct PendingBatchRow {
    flight_key: String,
    row: FlightState,
}

#[derive(Default)]
struct BatchStats {
    rows_inserted: usize,
    rows_updated: usize,
}

enum BatchRetryError {
    RetryableSerialization { row_id: Option<usize> },
    Fatal(String),
}

#[derive(Clone)]
struct FlightIndexes {
    flight_id: Arc<SecondaryIndex<usize>>,
    altitude: Arc<SecondaryIndex<usize>>,
    gs: Arc<SecondaryIndex<usize>>,
    lat: Arc<SecondaryIndex<usize>>,
    lon: Arc<SecondaryIndex<usize>>,
    updated_at: Arc<SecondaryIndex<usize>>,
}

impl FlightIndexes {
    fn new(shm: Arc<ShmArena>) -> Self {
        Self {
            flight_id: Arc::new(SecondaryIndex::new_in_shared("flight_id", Arc::clone(&shm))),
            altitude: Arc::new(SecondaryIndex::new_in_shared("altitude", Arc::clone(&shm))),
            gs: Arc::new(SecondaryIndex::new_in_shared("gs", Arc::clone(&shm))),
            lat: Arc::new(SecondaryIndex::new_in_shared("lat", Arc::clone(&shm))),
            lon: Arc::new(SecondaryIndex::new_in_shared("lon", Arc::clone(&shm))),
            updated_at: Arc::new(SecondaryIndex::new_in_shared("updated_at", shm)),
        }
    }

    fn from_layout(shm: Arc<ShmArena>, layout: &BootLayout) -> Result<Self, String> {
        if (layout.index_count as usize) < INDEX_SLOT_COUNT {
            return Err(format!(
                "boot layout index_count={} is missing required indexes ({})",
                layout.index_count, INDEX_SLOT_COUNT
            ));
        }
        if layout.index_count as usize > BOOT_LAYOUT_MAX_INDEXES {
            return Err(format!(
                "boot layout index_count={} exceeds max {}",
                layout.index_count, BOOT_LAYOUT_MAX_INDEXES
            ));
        }

        let require = |slot: usize| -> Result<u32, String> {
            let offset = layout.index_offsets.get(slot).copied().unwrap_or(0);
            if offset == 0 {
                return Err(format!("boot layout index slot {} has zero offset", slot));
            }
            Ok(offset)
        };

        let flight_id = Arc::new(
            SecondaryIndex::from_existing(
                "flight_id",
                Arc::clone(&shm),
                require(INDEX_SLOT_FLIGHT_ID)?,
            )
            .map_err(|err| format!("failed to attach flight_id index from boot layout: {}", err))?,
        );
        let altitude = Arc::new(
            SecondaryIndex::from_existing(
                "altitude",
                Arc::clone(&shm),
                require(INDEX_SLOT_ALTITUDE)?,
            )
            .map_err(|err| format!("failed to attach altitude index from boot layout: {}", err))?,
        );
        let gs = Arc::new(
            SecondaryIndex::from_existing("gs", Arc::clone(&shm), require(INDEX_SLOT_GS)?)
                .map_err(|err| format!("failed to attach gs index from boot layout: {}", err))?,
        );
        let lat = Arc::new(
            SecondaryIndex::from_existing("lat", Arc::clone(&shm), require(INDEX_SLOT_LAT)?)
                .map_err(|err| format!("failed to attach lat index from boot layout: {}", err))?,
        );
        let lon = Arc::new(
            SecondaryIndex::from_existing("lon", Arc::clone(&shm), require(INDEX_SLOT_LON)?)
                .map_err(|err| format!("failed to attach lon index from boot layout: {}", err))?,
        );
        let updated_at = Arc::new(
            SecondaryIndex::from_existing("updated_at", shm, require(INDEX_SLOT_UPDATED_AT)?)
                .map_err(|err| {
                    format!(
                        "failed to attach updated_at index from boot layout: {}",
                        err
                    )
                })?,
        );

        Ok(Self {
            flight_id,
            altitude,
            gs,
            lat,
            lon,
            updated_at,
        })
    }

    fn write_layout_offsets(&self, layout: &mut BootLayout) -> Result<(), String> {
        if BOOT_LAYOUT_MAX_INDEXES < INDEX_SLOT_COUNT {
            return Err(format!(
                "BOOT_LAYOUT_MAX_INDEXES={} is smaller than required {}",
                BOOT_LAYOUT_MAX_INDEXES, INDEX_SLOT_COUNT
            ));
        }
        layout.index_offsets[INDEX_SLOT_FLIGHT_ID] = self.flight_id.header_offset();
        layout.index_offsets[INDEX_SLOT_ALTITUDE] = self.altitude.header_offset();
        layout.index_offsets[INDEX_SLOT_GS] = self.gs.header_offset();
        layout.index_offsets[INDEX_SLOT_LAT] = self.lat.header_offset();
        layout.index_offsets[INDEX_SLOT_LON] = self.lon.header_offset();
        layout.index_offsets[INDEX_SLOT_UPDATED_AT] = self.updated_at.header_offset();
        layout.index_count = INDEX_SLOT_COUNT as u32;
        Ok(())
    }

    fn as_catalog(&self, pk_map: Arc<ShmPrimaryKeyMap>) -> SchemaCatalog {
        let mut catalog = SchemaCatalog::new("flight_id").with_primary_key_map(pk_map);

        catalog.insert_index("flight_id", Arc::clone(&self.flight_id));
        catalog.insert_index("flight", Arc::clone(&self.flight_id));
        catalog.insert_index("ident", Arc::clone(&self.flight_id));

        catalog.insert_index("altitude", Arc::clone(&self.altitude));
        catalog.insert_index("alt", Arc::clone(&self.altitude));

        catalog.insert_index("lat", Arc::clone(&self.lat));

        catalog.insert_index("lon", Arc::clone(&self.lon));
        catalog.insert_index("long", Arc::clone(&self.lon));
        catalog.insert_index("longitude", Arc::clone(&self.lon));

        catalog.insert_index("gs", Arc::clone(&self.gs));
        catalog.insert_index("groundspeed", Arc::clone(&self.gs));

        catalog.insert_index("updated_at", Arc::clone(&self.updated_at));
        catalog.insert_index("updated", Arc::clone(&self.updated_at));
        catalog.insert_index("ts", Arc::clone(&self.updated_at));
        catalog.set_cardinality_rank("flight_id", 0);
        catalog.set_cardinality_rank("dest", 2);
        catalog.set_cardinality_rank("altitude", 3);
        catalog.set_cardinality_rank("alt", 3);

        catalog
    }

    fn remove_row(&self, row_id: usize, row: &FlightState) {
        if row.exists == 0 {
            return;
        }

        let row_key = row.flight_id_string();
        self.flight_id.remove(&IndexValue::String(row_key), &row_id);

        self.altitude
            .remove(&IndexValue::I64(row.altitude as i64), &row_id);
        self.gs.remove(&IndexValue::I64(row.gs as i64), &row_id);
        self.lat.remove(&IndexValue::I64(row.lat_scaled), &row_id);
        self.lon.remove(&IndexValue::I64(row.lon_scaled), &row_id);
        if let Ok(updated_at) = i64::try_from(row.updated_at) {
            self.updated_at
                .remove(&IndexValue::I64(updated_at), &row_id);
        }
    }

    fn insert_row(&self, row_id: usize, row: &FlightState) {
        if row.exists == 0 {
            return;
        }

        let row_key = row.flight_id_string();
        self.flight_id.insert(IndexValue::String(row_key), row_id);

        self.altitude
            .insert(IndexValue::I64(row.altitude as i64), row_id);
        self.gs.insert(IndexValue::I64(row.gs as i64), row_id);
        self.lat.insert(IndexValue::I64(row.lat_scaled), row_id);
        self.lon.insert(IndexValue::I64(row.lon_scaled), row_id);
        if let Ok(updated_at) = i64::try_from(row.updated_at) {
            self.updated_at.insert(IndexValue::I64(updated_at), row_id);
        }
    }
}

struct WalRuntime {
    configured_mode: SynchronousCommit,
    committer: aerostore_core::OccCommitter<WAL_RING_SLOTS, WAL_RING_SLOT_BYTES>,
    ring: SharedWalRing<WAL_RING_SLOTS, WAL_RING_SLOT_BYTES>,
    _wal_daemon: aerostore_core::WalWriterDaemon,
}

struct SharedFlightDb {
    _shm: Arc<ShmArena>,
    table: Arc<OccTable<FlightState>>,
    optimizer: RuleBasedOptimizer<FlightState>,
    key_index: Arc<ShmPrimaryKeyMap>,
    row_capacity: usize,
    indexes: FlightIndexes,
    wal_runtime: Mutex<WalRuntime>,
    wal_path: PathBuf,
    checkpoint_path: PathBuf,
    checkpoint_interval_secs: AtomicU64,
    checkpointer_started: AtomicBool,
    boot_mode: BootMode,
    orphaned_proc_slots_cleared: usize,
    _data_dir: PathBuf,
}

impl SharedFlightDb {
    fn open(data_dir: &Path) -> Result<Self, String> {
        std::fs::create_dir_all(data_dir).map_err(|err| {
            format!(
                "failed to create data dir '{}': {}",
                data_dir.display(),
                err
            )
        })?;

        let wal_path = data_dir.join(WAL_FILE_NAME);
        let checkpoint_path = data_dir.join(CHECKPOINT_FILE_NAME);
        let shm_path = resolve_shm_path();
        let boot_context = open_boot_context(Some(shm_path.as_path()), DEFAULT_SHM_ARENA_BYTES)
            .map_err(|err| {
                format!(
                    "failed to open shared tmpfs arena '{}': {}",
                    shm_path.display(),
                    err
                )
            })?;

        let shm = Arc::clone(&boot_context.shm);
        let mut boot_mode = boot_context.mode;
        let orphaned_proc_slots_cleared = boot_context.orphaned_proc_slots_cleared;

        let (table, indexes, key_index, optimizer, ring) = if boot_mode == BootMode::WarmAttach {
            let layout = load_boot_layout(shm.as_ref())
                .map_err(|err| format!("failed to load warm boot layout: {}", err))?
                .ok_or_else(|| {
                    "warm boot expected persisted layout, but none was found".to_string()
                })?;
            match Self::attach_warm_state(Arc::clone(&shm), &layout) {
                Ok(state) => state,
                Err(warm_err) => {
                    shm.reinitialize_header().map_err(|err| {
                        format!(
                            "warm attach failed ({warm_err}); could not reinitialize shared arena: {}",
                            err
                        )
                    })?;
                    clear_persisted_boot_layout(shm.as_ref());
                    boot_mode = BootMode::ColdReplay;
                    Self::initialize_cold_state(
                        Arc::clone(&shm),
                        checkpoint_path.as_path(),
                        wal_path.as_path(),
                    )?
                }
            }
        } else {
            clear_persisted_boot_layout(shm.as_ref());
            Self::initialize_cold_state(
                Arc::clone(&shm),
                checkpoint_path.as_path(),
                wal_path.as_path(),
            )?
        };

        let wal_daemon = spawn_wal_writer_daemon(ring.clone(), &wal_path)
            .map_err(|err| format!("failed to spawn WAL writer daemon: {}", err))?;
        let committer =
            aerostore_core::OccCommitter::<WAL_RING_SLOTS, WAL_RING_SLOT_BYTES>::new_synchronous(
                &wal_path,
            )
            .map_err(|err| format!("failed to initialize synchronous committer: {}", err))?;
        let wal_runtime = WalRuntime {
            configured_mode: SynchronousCommit::On,
            committer,
            ring,
            _wal_daemon: wal_daemon,
        };

        Ok(Self {
            _shm: shm,
            table,
            optimizer,
            key_index,
            row_capacity: DEFAULT_ROW_CAPACITY,
            indexes,
            wal_runtime: Mutex::new(wal_runtime),
            wal_path,
            checkpoint_path,
            checkpoint_interval_secs: AtomicU64::new(DEFAULT_CHECKPOINT_INTERVAL_SECS),
            checkpointer_started: AtomicBool::new(false),
            boot_mode,
            orphaned_proc_slots_cleared,
            _data_dir: data_dir.to_path_buf(),
        })
    }

    fn initialize_cold_state(
        shm: Arc<ShmArena>,
        checkpoint_path: &Path,
        wal_path: &Path,
    ) -> Result<
        (
            Arc<OccTable<FlightState>>,
            FlightIndexes,
            Arc<ShmPrimaryKeyMap>,
            RuleBasedOptimizer<FlightState>,
            SharedWalRing<WAL_RING_SLOTS, WAL_RING_SLOT_BYTES>,
        ),
        String,
    > {
        let table = Arc::new(
            OccTable::<FlightState>::new(Arc::clone(&shm), DEFAULT_ROW_CAPACITY)
                .map_err(|err| format!("failed to create OCC table: {}", err))?,
        );
        for row_id in 0..DEFAULT_ROW_CAPACITY {
            table
                .seed_row(row_id, FlightState::empty())
                .map_err(|err| format!("failed to seed OCC row {}: {}", row_id, err))?;
        }

        let indexes = FlightIndexes::new(Arc::clone(&shm));
        let key_index = Arc::new(
            ShmPrimaryKeyMap::new_in_shared(Arc::clone(&shm), 4096, DEFAULT_ROW_CAPACITY)
                .map_err(|err| format!("failed to create shared primary key map: {}", err))?,
        );

        let _recovery = recover_occ_table_from_checkpoint_and_wal_with_pk_map(
            &table,
            Some(key_index.as_ref()),
            checkpoint_path,
            wal_path,
        )
        .map_err(|err| format!("failed to recover OCC state from checkpoint/WAL: {}", err))?;

        for row_id in 0..DEFAULT_ROW_CAPACITY {
            let row = table
                .latest_value(row_id)
                .map_err(|err| format!("failed to inspect recovered row {}: {}", row_id, err))?
                .ok_or_else(|| format!("seeded row {} unexpectedly missing", row_id))?;
            if row.exists == 0 {
                continue;
            }
            indexes.insert_row(row_id, &row);
            key_index
                .insert_existing(row.flight_id_string().as_str(), row_id)
                .map_err(|err| format!("failed to rebuild primary key map: {}", err))?;
        }

        let ring = SharedWalRing::<WAL_RING_SLOTS, WAL_RING_SLOT_BYTES>::create(Arc::clone(&shm))
            .map_err(|err| format!("failed to create shared WAL ring: {}", err))?;
        Self::persist_layout(shm.as_ref(), &table, key_index.as_ref(), &indexes, &ring)?;

        let optimizer = RuleBasedOptimizer::new(indexes.as_catalog(Arc::clone(&key_index)));
        Ok((table, indexes, key_index, optimizer, ring))
    }

    fn attach_warm_state(
        shm: Arc<ShmArena>,
        layout: &BootLayout,
    ) -> Result<
        (
            Arc<OccTable<FlightState>>,
            FlightIndexes,
            Arc<ShmPrimaryKeyMap>,
            RuleBasedOptimizer<FlightState>,
            SharedWalRing<WAL_RING_SLOTS, WAL_RING_SLOT_BYTES>,
        ),
        String,
    > {
        if layout.row_capacity as usize != DEFAULT_ROW_CAPACITY {
            return Err(format!(
                "boot layout row_capacity {} does not match runtime capacity {}",
                layout.row_capacity, DEFAULT_ROW_CAPACITY
            ));
        }

        let occ_slot_offsets = read_u32_array(
            shm.as_ref(),
            layout.occ_slot_offsets_offset,
            layout.occ_slot_offsets_len,
        )
        .map_err(|err| format!("failed to read OCC slot offsets from boot layout: {}", err))?;
        let table = Arc::new(
            OccTable::<FlightState>::from_existing(
                Arc::clone(&shm),
                layout.occ_shared_header_offset,
                occ_slot_offsets,
            )
            .map_err(|err| format!("failed to attach OCC table from boot layout: {}", err))?,
        );

        let pk_bucket_offsets = read_u32_array(
            shm.as_ref(),
            layout.pk_bucket_offsets_offset,
            layout.pk_bucket_offsets_len,
        )
        .map_err(|err| format!("failed to read PK bucket offsets from boot layout: {}", err))?;
        let key_index = Arc::new(
            ShmPrimaryKeyMap::from_existing(
                Arc::clone(&shm),
                layout.pk_header_offset,
                pk_bucket_offsets,
            )
            .map_err(|err| format!("failed to attach primary key map from boot layout: {}", err))?,
        );

        let indexes = FlightIndexes::from_layout(Arc::clone(&shm), layout)?;
        let optimizer = RuleBasedOptimizer::new(indexes.as_catalog(Arc::clone(&key_index)));

        if layout.wal_ring_offset == 0 {
            return Err("boot layout WAL ring offset is zero".to_string());
        }
        let ring_ptr = RelPtr::<WalRing<WAL_RING_SLOTS, WAL_RING_SLOT_BYTES>>::from_offset(
            layout.wal_ring_offset,
        );
        let ring = SharedWalRing::<WAL_RING_SLOTS, WAL_RING_SLOT_BYTES>::from_existing(
            Arc::clone(&shm),
            ring_ptr,
        );
        ring.reset_for_restart()
            .map_err(|err| format!("failed to reset warm-attached WAL ring: {}", err))?;

        Ok((table, indexes, key_index, optimizer, ring))
    }

    fn persist_layout(
        shm: &ShmArena,
        table: &OccTable<FlightState>,
        key_index: &ShmPrimaryKeyMap,
        indexes: &FlightIndexes,
        ring: &SharedWalRing<WAL_RING_SLOTS, WAL_RING_SLOT_BYTES>,
    ) -> Result<(), String> {
        let mut layout = BootLayout::new(DEFAULT_ROW_CAPACITY)
            .map_err(|err| format!("failed to create boot layout: {}", err))?;

        layout.occ_shared_header_offset = table.shared_header_offset();

        let occ_slot_offsets = table.index_slot_offsets();
        let (occ_slot_offsets_offset, occ_slot_offsets_len) =
            alloc_u32_array(shm, occ_slot_offsets.as_slice()).map_err(|err| {
                format!("failed to store OCC slot offsets in shared layout: {}", err)
            })?;
        layout.occ_slot_offsets_offset = occ_slot_offsets_offset;
        layout.occ_slot_offsets_len = occ_slot_offsets_len;

        layout.pk_header_offset = key_index.header_offset();
        let pk_bucket_offsets = key_index.bucket_offsets();
        let (pk_bucket_offsets_offset, pk_bucket_offsets_len) =
            alloc_u32_array(shm, pk_bucket_offsets.as_slice())
                .map_err(|err| format!("failed to store PK offsets in shared layout: {}", err))?;
        layout.pk_bucket_offsets_offset = pk_bucket_offsets_offset;
        layout.pk_bucket_offsets_len = pk_bucket_offsets_len;

        layout.wal_ring_offset = ring.ring_ptr().load(AtomicOrdering::Acquire);
        indexes.write_layout_offsets(&mut layout)?;

        persist_shared_boot_layout(shm, &layout)
            .map_err(|err| format!("failed to persist shared boot layout: {}", err))?;
        Ok(())
    }

    fn start_checkpointer(self: &Arc<Self>) {
        if self
            .checkpointer_started
            .compare_exchange(false, true, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
            .is_err()
        {
            return;
        }

        let db = Arc::clone(self);
        std::thread::spawn(move || loop {
            let interval_secs = db.checkpoint_interval_secs.load(AtomicOrdering::Acquire);
            if interval_secs == 0 {
                std::thread::sleep(Duration::from_millis(250));
                continue;
            }

            std::thread::sleep(Duration::from_secs(interval_secs));
            let mode = match db.current_synchronous_commit_mode() {
                Ok(mode) => mode,
                Err(_) => continue,
            };
            if mode == SynchronousCommit::Off {
                continue;
            }
            let _ = db.checkpoint_now();
        });
    }

    fn set_synchronous_commit_mode(&self, mode: SynchronousCommit) -> Result<(), String> {
        let mut runtime = self
            .wal_runtime
            .lock()
            .map_err(|_| "wal runtime lock poisoned".to_string())?;

        if runtime.configured_mode == mode {
            return Ok(());
        }

        runtime.committer = match mode {
            SynchronousCommit::On => aerostore_core::OccCommitter::<
                WAL_RING_SLOTS,
                WAL_RING_SLOT_BYTES,
            >::new_synchronous(&self.wal_path)
            .map_err(|err| format!("failed to enable synchronous commit mode: {}", err))?,
            SynchronousCommit::Off => aerostore_core::OccCommitter::<
                WAL_RING_SLOTS,
                WAL_RING_SLOT_BYTES,
            >::new_asynchronous(runtime.ring.clone()),
        };
        runtime.configured_mode = mode;
        Ok(())
    }

    fn current_synchronous_commit_mode(&self) -> Result<SynchronousCommit, String> {
        let runtime = self
            .wal_runtime
            .lock()
            .map_err(|_| "wal runtime lock poisoned".to_string())?;
        Ok(runtime.configured_mode)
    }

    fn set_checkpoint_interval_secs(&self, secs: u64) {
        self.checkpoint_interval_secs
            .store(secs, AtomicOrdering::Release);
    }

    #[inline]
    fn boot_mode(&self) -> BootMode {
        self.boot_mode
    }

    #[inline]
    fn orphaned_proc_slots_cleared(&self) -> usize {
        self.orphaned_proc_slots_cleared
    }

    fn checkpoint_interval_secs(&self) -> u64 {
        self.checkpoint_interval_secs.load(AtomicOrdering::Acquire)
    }

    fn checkpoint_now(&self) -> Result<usize, String> {
        let mut runtime = self
            .wal_runtime
            .lock()
            .map_err(|_| "wal runtime lock poisoned".to_string())?;

        if runtime.configured_mode == SynchronousCommit::Off {
            return Err(
                "checkpoint requires synchronous_commit=on (set aerostore.synchronous_commit first)"
                    .to_string(),
            );
        }

        runtime.committer =
            aerostore_core::OccCommitter::<WAL_RING_SLOTS, WAL_RING_SLOT_BYTES>::new_synchronous(
                &self.wal_path,
            )
            .map_err(|err| format!("failed to prepare synchronous checkpoint path: {}", err))?;

        write_occ_checkpoint_and_truncate_wal(&self.table, &self.checkpoint_path, &self.wal_path)
            .map_err(|err| format!("checkpoint failed: {}", err))
    }

    fn search_count(&self, request: SearchRequest) -> Result<usize, String> {
        let plan = self
            .optimizer
            .compile_from_stapi(request.stapi.as_str())
            .map_err(|err| format_planner_error(err))?;

        let mut tx = self
            .table
            .begin_transaction()
            .map_err(|err| format!("begin_transaction failed: {}", err))?;

        let mut rows = plan
            .execute(&self.table, &mut tx)
            .map_err(|err| err.tcl_error_message())?;

        self.table
            .abort(&mut tx)
            .map_err(|err| format!("abort failed: {}", err))?;

        rows.retain(|row| row.exists != 0);

        if request.has_sort && request.sort_desc {
            rows.reverse();
        }

        let start = request.offset.min(rows.len());
        let end = request
            .limit
            .map(|limit| start.saturating_add(limit).min(rows.len()))
            .unwrap_or(rows.len());

        Ok(end.saturating_sub(start))
    }

    fn ingest_tsv(&self, input: &[u8], batch_size: usize) -> Result<IngestStats, String> {
        if batch_size == 0 {
            return Err("batch size must be > 0".to_string());
        }

        let decoder = FlightTsvDecoder;
        let started = Instant::now();
        let mut stats = IngestStats::default();
        let mut batch_rows = Vec::<PendingBatchRow>::with_capacity(batch_size);

        let mut cursor = 0_usize;
        let mut line_no = 0_usize;

        while cursor < input.len() {
            let mut line_end = cursor;
            while line_end < input.len() && input[line_end] != b'\n' {
                line_end += 1;
            }

            let mut line = &input[cursor..line_end];
            cursor = if line_end < input.len() {
                line_end + 1
            } else {
                line_end
            };
            line_no += 1;

            if matches!(line.last(), Some(b'\r')) {
                line = &line[..line.len().saturating_sub(1)];
            }

            if line.is_empty() {
                continue;
            }

            let mut cols = TsvColumns::new(line);
            let (flight_key, row) = decoder
                .decode(&mut cols)
                .map_err(|err| format_decode_error(line_no, err))?;
            cols.ensure_end()
                .map_err(|err| format_decode_error(line_no, err))?;

            stats.rows_seen += 1;
            batch_rows.push(PendingBatchRow { flight_key, row });

            if batch_rows.len() >= batch_size {
                self.apply_batch_with_retry(batch_rows.as_slice(), &mut stats)?;
                stats.batches_committed += 1;
                batch_rows.clear();
            }
        }

        if !batch_rows.is_empty() {
            self.apply_batch_with_retry(batch_rows.as_slice(), &mut stats)?;
            stats.batches_committed += 1;
        }

        stats.elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        Ok(stats)
    }

    fn apply_batch_with_retry(
        &self,
        batch_rows: &[PendingBatchRow],
        stats: &mut IngestStats,
    ) -> Result<(), String> {
        if batch_rows.is_empty() {
            return Ok(());
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let mut backoff = RetryBackoff::with_seed(
            now ^ ((std::process::id() as u64) << 17),
            RetryPolicy::hot_key_default(),
        );
        let policy = backoff.policy();
        let mut conflict_streak_by_row = HashMap::<usize, u32>::new();
        let mut touched_rows = Vec::<usize>::with_capacity(batch_rows.len());

        for attempt in 0..MAX_BATCH_RETRY_ATTEMPTS {
            let mut tx = self
                .table
                .begin_transaction()
                .map_err(|err| format!("begin_transaction failed: {}", err))?;
            let mut pending_index_updates: HashMap<usize, PendingIndexUpdate> = HashMap::new();
            let mut batch_stats = BatchStats::default();
            let mut touched_set = HashSet::<usize>::with_capacity(batch_rows.len());
            let mut locked_row_ids = HashSet::<usize>::new();
            let mut row_locks = Vec::new();
            touched_rows.clear();

            let mut attempt_result = Ok(());
            for pending in batch_rows {
                let row_id = self.resolve_or_allocate_row_id(pending.flight_key.as_str())?;
                if touched_set.insert(row_id) {
                    touched_rows.push(row_id);
                }

                let streak = conflict_streak_by_row.get(&row_id).copied().unwrap_or(0);
                if streak >= policy.escalate_after_failures && locked_row_ids.insert(row_id) {
                    match self.table.lock_for_update(&tx, row_id) {
                        Ok(guard) => row_locks.push(guard),
                        Err(OccError::SerializationFailure) => {
                            attempt_result = Err(BatchRetryError::RetryableSerialization {
                                row_id: Some(row_id),
                            });
                            break;
                        }
                        Err(err) => {
                            attempt_result = Err(BatchRetryError::Fatal(format!(
                                "lock_for_update failed for row {}: {}",
                                row_id, err
                            )));
                            break;
                        }
                    }
                }

                attempt_result = self.upsert_one(
                    &mut tx,
                    &mut pending_index_updates,
                    row_id,
                    pending.row,
                    &mut batch_stats,
                );
                if attempt_result.is_err() {
                    break;
                }
            }

            if attempt_result.is_ok() {
                attempt_result = self.commit_batch(&mut tx, &mut pending_index_updates);
            }

            match attempt_result {
                Ok(()) => {
                    stats.rows_inserted += batch_stats.rows_inserted;
                    stats.rows_updated += batch_stats.rows_updated;
                    for row_id in &touched_rows {
                        conflict_streak_by_row.remove(row_id);
                    }
                    return Ok(());
                }
                Err(BatchRetryError::Fatal(message)) => {
                    let _ = self.table.abort(&mut tx);
                    return Err(message);
                }
                Err(BatchRetryError::RetryableSerialization { row_id }) => {
                    let _ = self.table.abort(&mut tx);

                    if let Some(row_id) = row_id {
                        let next = conflict_streak_by_row
                            .get(&row_id)
                            .copied()
                            .unwrap_or(0)
                            .saturating_add(1);
                        conflict_streak_by_row.insert(row_id, next);
                    } else {
                        for touched in &touched_rows {
                            let next = conflict_streak_by_row
                                .get(touched)
                                .copied()
                                .unwrap_or(0)
                                .saturating_add(1);
                            conflict_streak_by_row.insert(*touched, next);
                        }
                    }
                }
            }

            if attempt + 1 >= MAX_BATCH_RETRY_ATTEMPTS {
                return Err(format!(
                    "serialization failure during OCC commit after {} retries",
                    MAX_BATCH_RETRY_ATTEMPTS
                ));
            }

            backoff.sleep_for_attempt(attempt);
            std::thread::yield_now();
        }

        Err(format!(
            "serialization failure during OCC commit after {} retries",
            MAX_BATCH_RETRY_ATTEMPTS
        ))
    }

    fn upsert_one(
        &self,
        tx: &mut OccTransaction<FlightState>,
        pending_index_updates: &mut HashMap<usize, PendingIndexUpdate>,
        row_id: usize,
        next_row: FlightState,
        batch_stats: &mut BatchStats,
    ) -> Result<(), BatchRetryError> {
        let current = match self.table.read(tx, row_id) {
            Ok(Some(value)) => value,
            Ok(None) => {
                return Err(BatchRetryError::Fatal(format!(
                    "row {} is unexpectedly missing",
                    row_id
                )));
            }
            Err(OccError::SerializationFailure) => {
                return Err(BatchRetryError::RetryableSerialization {
                    row_id: Some(row_id),
                });
            }
            Err(err) => {
                return Err(BatchRetryError::Fatal(format!(
                    "read failed for row {}: {}",
                    row_id, err
                )));
            }
        };

        let was_live = current.exists != 0;

        match self.table.write(tx, row_id, next_row) {
            Ok(()) => {}
            Err(OccError::SerializationFailure) => {
                return Err(BatchRetryError::RetryableSerialization {
                    row_id: Some(row_id),
                });
            }
            Err(err) => {
                return Err(BatchRetryError::Fatal(format!(
                    "write failed for row {}: {}",
                    row_id, err
                )));
            }
        }

        pending_index_updates
            .entry(row_id)
            .and_modify(|update| update.after = next_row)
            .or_insert(PendingIndexUpdate {
                before: current,
                after: next_row,
            });

        if was_live {
            batch_stats.rows_updated += 1;
        } else {
            batch_stats.rows_inserted += 1;
        }

        Ok(())
    }

    fn resolve_or_allocate_row_id(&self, flight_key: &str) -> Result<usize, String> {
        let row_id = self
            .key_index
            .get_or_insert(flight_key)
            .map_err(|err| format!("failed to resolve row id for '{}': {}", flight_key, err))?;

        if row_id >= self.row_capacity {
            return Err(format!(
                "assigned row id {} exceeds capacity {}",
                row_id, self.row_capacity
            ));
        }

        Ok(row_id)
    }

    fn commit_batch(
        &self,
        tx: &mut OccTransaction<FlightState>,
        pending_index_updates: &mut HashMap<usize, PendingIndexUpdate>,
    ) -> Result<(), BatchRetryError> {
        let commit_result = {
            let mut guard = self
                .wal_runtime
                .lock()
                .map_err(|_| BatchRetryError::Fatal("wal runtime lock poisoned".to_string()))?;
            guard.committer.commit(&self.table, tx)
        };

        match commit_result {
            Ok(_) => {
                for (row_id, update) in pending_index_updates.drain() {
                    self.indexes.remove_row(row_id, &update.before);
                    self.indexes.insert_row(row_id, &update.after);
                }
                Ok(())
            }
            Err(WalWriterError::Occ(OccError::SerializationFailure)) => {
                pending_index_updates.clear();
                Err(BatchRetryError::RetryableSerialization { row_id: None })
            }
            Err(err) => {
                pending_index_updates.clear();
                Err(BatchRetryError::Fatal(format!("commit failed: {}", err)))
            }
        }
    }
}

#[inline]
fn encode_fixed_ascii<const N: usize>(value: &str) -> Option<[u8; N]> {
    if value.len() > N {
        return None;
    }

    let mut out = [0_u8; N];
    let bytes = value.as_bytes();
    out[..bytes.len()].copy_from_slice(bytes);
    Some(out)
}

#[inline]
fn decode_fixed_ascii(value: &[u8]) -> String {
    let end = value.iter().position(|b| *b == 0).unwrap_or(value.len());
    String::from_utf8_lossy(&value[..end]).to_string()
}

#[inline]
fn scale_coord(value: f64) -> i64 {
    (value * 1_000_000.0).round() as i64
}

fn format_decode_error(line: usize, err: TsvDecodeError) -> String {
    format!(
        "decode error at line {} column {}: {}",
        line, err.column, err.message
    )
}

fn format_planner_error(err: PlannerError) -> String {
    err.tcl_error_message()
}

fn with_ffi_boundary(interp: *mut clib::Tcl_Interp, f: impl FnOnce() -> c_int) -> c_int {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(code) => code,
        Err(_) => unsafe { set_error(interp, "aerostore Tcl callback panicked") },
    }
}

#[no_mangle]
pub extern "C" fn Aerostore_Init(interp: *mut clib::Tcl_Interp) -> c_int {
    with_ffi_boundary(interp, || unsafe { aerostore_init(interp) })
}

#[no_mangle]
pub extern "C" fn Aerostore_SafeInit(interp: *mut clib::Tcl_Interp) -> c_int {
    Aerostore_Init(interp)
}

unsafe fn aerostore_init(interp_ptr: *mut clib::Tcl_Interp) -> c_int {
    let interp = match Interp::from_raw(interp_ptr) {
        Ok(interp) => interp,
        Err(_) => return set_error(interp_ptr, "received null Tcl interpreter"),
    };

    if let Err(err) = interp.run("namespace eval aerostore {}") {
        return set_error(
            interp_ptr,
            &format!("failed to create aerostore namespace: {err}"),
        );
    }

    interp.def_proc("aerostore::init", aerostore_init_cmd);
    interp.def_proc("aerostore::set_config", aerostore_set_config_cmd);
    interp.def_proc("aerostore::get_config", aerostore_get_config_cmd);
    interp.def_proc("aerostore::checkpoint_now", aerostore_checkpoint_now_cmd);
    interp.def_proc("FlightState", flight_state_cmd);

    let rc = interp.package_provide(PACKAGE_NAME, PACKAGE_VERSION);
    if rc != clib::TCL_OK as c_int {
        return set_error(interp_ptr, "Tcl_PkgProvide(aerostore) failed");
    }

    clib::TCL_OK as c_int
}

extern "C" fn aerostore_init_cmd(
    _client_data: clib::ClientData,
    interp: *mut clib::Tcl_Interp,
    objc: c_int,
    objv: *const *mut clib::Tcl_Obj,
) -> c_int {
    with_ffi_boundary(interp, || unsafe {
        match aerostore_init_cmd_impl(interp, objc, objv) {
            Ok(code) => code,
            Err(message) => set_error(interp, &message),
        }
    })
}

unsafe fn aerostore_init_cmd_impl(
    interp: *mut clib::Tcl_Interp,
    objc: c_int,
    objv: *const *mut clib::Tcl_Obj,
) -> Result<c_int, String> {
    let args = arg_slice(objc, objv)?;
    if args.len() > 2 {
        return Err("usage: aerostore::init ?data_dir?".to_string());
    }

    let requested_dir = if args.len() == 2 {
        Some(obj_to_str(args[1])?.to_string())
    } else {
        None
    };

    let db = ensure_database(requested_dir.as_deref())?;
    let dir_msg = GLOBAL_DB_DIR
        .get()
        .cloned()
        .unwrap_or_else(|| DEFAULT_DATA_DIR.to_string());
    let boot_mode = match db.boot_mode() {
        BootMode::WarmAttach => "warm",
        BootMode::ColdReplay => "cold",
    };

    Ok(set_ok_result(
        interp,
        &format!(
            "aerostore ready (dir={dir_msg}, occ_table={:p}, boot={}, procarray_orphans_cleared={})",
            Arc::as_ptr(db),
            boot_mode,
            db.orphaned_proc_slots_cleared(),
        ),
    ))
}

fn ensure_database(data_dir: Option<&str>) -> Result<&'static Arc<FlightDb>, String> {
    if let Some(db) = GLOBAL_DB.get() {
        db.start_checkpointer();
        return Ok(db);
    }

    let dir = data_dir.unwrap_or(DEFAULT_DATA_DIR);
    let db = SharedFlightDb::open(Path::new(dir))?;

    let _ = GLOBAL_DB_DIR.set(dir.to_string());
    let _ = GLOBAL_DB.set(Arc::new(db));
    let db = GLOBAL_DB
        .get()
        .ok_or_else(|| "failed to initialize global database".to_string())?;
    db.start_checkpointer();
    Ok(db)
}

fn resolve_shm_path() -> PathBuf {
    std::env::var(SHM_PATH_ENV_KEY)
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_TMPFS_PATH))
}

extern "C" fn aerostore_set_config_cmd(
    _client_data: clib::ClientData,
    interp: *mut clib::Tcl_Interp,
    objc: c_int,
    objv: *const *mut clib::Tcl_Obj,
) -> c_int {
    with_ffi_boundary(interp, || unsafe {
        match aerostore_set_config_cmd_impl(interp, objc, objv) {
            Ok(code) => code,
            Err(message) => set_error(interp, &message),
        }
    })
}

unsafe fn aerostore_set_config_cmd_impl(
    interp: *mut clib::Tcl_Interp,
    objc: c_int,
    objv: *const *mut clib::Tcl_Obj,
) -> Result<c_int, String> {
    let args = arg_slice(objc, objv)?;
    if args.len() != 3 {
        return Err("usage: aerostore::set_config <key> <value>".to_string());
    }

    let key = obj_to_str(args[1])?;
    let db = ensure_database(None)?;
    match key {
        SYNCHRONOUS_COMMIT_KEY => {
            let mode = SynchronousCommit::from_setting(obj_to_str(args[2])?);
            db.set_synchronous_commit_mode(mode)?;
        }
        CHECKPOINT_INTERVAL_SECS_KEY => {
            let secs = parse_u64(interp, args[2])?;
            db.set_checkpoint_interval_secs(secs);
        }
        _ => {
            return Err(format!(
                "unknown config key '{}'; supported: {}, {}",
                key, SYNCHRONOUS_COMMIT_KEY, CHECKPOINT_INTERVAL_SECS_KEY
            ));
        }
    }

    Ok(set_ok_result(interp, "ok"))
}

extern "C" fn aerostore_get_config_cmd(
    _client_data: clib::ClientData,
    interp: *mut clib::Tcl_Interp,
    objc: c_int,
    objv: *const *mut clib::Tcl_Obj,
) -> c_int {
    with_ffi_boundary(interp, || unsafe {
        match aerostore_get_config_cmd_impl(interp, objc, objv) {
            Ok(code) => code,
            Err(message) => set_error(interp, &message),
        }
    })
}

unsafe fn aerostore_get_config_cmd_impl(
    interp: *mut clib::Tcl_Interp,
    objc: c_int,
    objv: *const *mut clib::Tcl_Obj,
) -> Result<c_int, String> {
    let args = arg_slice(objc, objv)?;
    if args.len() != 2 {
        return Err("usage: aerostore::get_config <key>".to_string());
    }

    let key = obj_to_str(args[1])?;
    let db = ensure_database(None)?;
    match key {
        SYNCHRONOUS_COMMIT_KEY => {
            let mode = db.current_synchronous_commit_mode()?;
            let value = if mode == SynchronousCommit::Off {
                "off"
            } else {
                "on"
            };
            Ok(set_ok_result(interp, value))
        }
        CHECKPOINT_INTERVAL_SECS_KEY => {
            let secs = db.checkpoint_interval_secs();
            Ok(set_wide_result(interp, secs as i64))
        }
        _ => Err(format!(
            "unknown config key '{}'; supported: {}, {}",
            key, SYNCHRONOUS_COMMIT_KEY, CHECKPOINT_INTERVAL_SECS_KEY
        )),
    }
}

extern "C" fn aerostore_checkpoint_now_cmd(
    _client_data: clib::ClientData,
    interp: *mut clib::Tcl_Interp,
    objc: c_int,
    objv: *const *mut clib::Tcl_Obj,
) -> c_int {
    with_ffi_boundary(interp, || unsafe {
        match aerostore_checkpoint_now_cmd_impl(interp, objc, objv) {
            Ok(code) => code,
            Err(message) => set_error(interp, &message),
        }
    })
}

unsafe fn aerostore_checkpoint_now_cmd_impl(
    interp: *mut clib::Tcl_Interp,
    objc: c_int,
    objv: *const *mut clib::Tcl_Obj,
) -> Result<c_int, String> {
    let args = arg_slice(objc, objv)?;
    if args.len() != 1 {
        return Err("usage: aerostore::checkpoint_now".to_string());
    }

    let db = ensure_database(None)?;
    let rows = db.checkpoint_now()?;
    Ok(set_ok_result(interp, &format!("checkpoint rows={rows}")))
}

extern "C" fn flight_state_cmd(
    _client_data: clib::ClientData,
    interp: *mut clib::Tcl_Interp,
    objc: c_int,
    objv: *const *mut clib::Tcl_Obj,
) -> c_int {
    with_ffi_boundary(interp, || unsafe {
        match flight_state_cmd_impl(interp, objc, objv) {
            Ok(code) => code,
            Err(message) => set_error(interp, &message),
        }
    })
}

unsafe fn flight_state_cmd_impl(
    interp: *mut clib::Tcl_Interp,
    objc: c_int,
    objv: *const *mut clib::Tcl_Obj,
) -> Result<c_int, String> {
    let args = arg_slice(objc, objv)?;
    if args.len() < 2 {
        return Err("usage: FlightState <search|ingest_tsv> ?options?".to_string());
    }

    let subcmd = obj_to_str(args[1])?;
    match subcmd {
        "search" => search_cmd(interp, &args),
        "ingest_tsv" => ingest_tsv_cmd(interp, &args),
        _ => Err("unknown subcommand; expected one of: search, ingest_tsv".to_string()),
    }
}

unsafe fn search_cmd(
    interp: *mut clib::Tcl_Interp,
    args: &[*mut clib::Tcl_Obj],
) -> Result<c_int, String> {
    let request = parse_search_request(interp, args)?;
    let db = ensure_database(None)?;
    let count = db.search_count(request)? as i64;
    Ok(set_wide_result(interp, count))
}

unsafe fn ingest_tsv_cmd(
    interp: *mut clib::Tcl_Interp,
    args: &[*mut clib::Tcl_Obj],
) -> Result<c_int, String> {
    if args.len() < 3 || args.len() > 4 {
        return Err("usage: FlightState ingest_tsv <tsv_data> ?batch_size?".to_string());
    }

    let db = ensure_database(None)?;
    let tsv_data = obj_to_bytes(args[2])?;
    let batch_size = if args.len() == 4 {
        parse_usize(interp, args[3])?
    } else {
        DEFAULT_INGEST_BATCH_SIZE
    };

    let stats = db.ingest_tsv(tsv_data, batch_size)?;
    Ok(set_ok_result(interp, &stats.metrics_line()))
}

unsafe fn parse_search_request(
    interp: *mut clib::Tcl_Interp,
    args: &[*mut clib::Tcl_Obj],
) -> Result<SearchRequest, String> {
    let mut stapi = String::new();
    let mut limit = None;
    let mut offset = 0_usize;
    let mut sort_desc = false;
    let mut has_sort = false;

    let mut i = 2_usize;
    while i < args.len() {
        let opt = obj_to_str(args[i])?;
        match opt {
            "-compare" => {
                if i + 1 >= args.len() {
                    return Err("missing value for -compare".to_string());
                }
                append_stapi_option(&mut stapi, "-compare", obj_to_str(args[i + 1])?);
                i += 2;
            }
            "-sort" => {
                if i + 1 >= args.len() {
                    return Err("missing value for -sort".to_string());
                }
                has_sort = true;
                append_stapi_option(&mut stapi, "-sort", obj_to_str(args[i + 1])?);
                i += 2;
            }
            "-limit" => {
                if i + 1 >= args.len() {
                    return Err("missing value for -limit".to_string());
                }
                limit = Some(parse_usize(interp, args[i + 1])?);
                i += 2;
            }
            "-offset" => {
                if i + 1 >= args.len() {
                    return Err("missing value for -offset".to_string());
                }
                offset = parse_usize(interp, args[i + 1])?;
                i += 2;
            }
            "-desc" => {
                sort_desc = true;
                i += 1;
            }
            "-asc" => {
                sort_desc = false;
                i += 1;
            }
            _ => return Err(format!("unknown search option: {opt}")),
        }
    }

    Ok(SearchRequest {
        stapi,
        limit,
        offset,
        sort_desc,
        has_sort,
    })
}

fn append_stapi_option(stapi: &mut String, option: &str, value: &str) {
    if !stapi.is_empty() {
        stapi.push(' ');
    }
    stapi.push_str(option);
    stapi.push(' ');
    stapi.push('{');
    stapi.push_str(value);
    stapi.push('}');
}

unsafe fn arg_slice(
    objc: c_int,
    objv: *const *mut clib::Tcl_Obj,
) -> Result<Vec<*mut clib::Tcl_Obj>, String> {
    if objc < 0 || objv.is_null() {
        return Err("invalid Tcl argument vector".to_string());
    }

    let argc = objc as usize;
    Ok(slice::from_raw_parts(objv, argc).to_vec())
}

unsafe fn parse_i64(interp: *mut clib::Tcl_Interp, obj: *mut clib::Tcl_Obj) -> Result<i64, String> {
    let mut out: clib::Tcl_WideInt = 0;
    let rc = clib::Tcl_GetWideIntFromObj(interp, obj, &mut out);
    if rc != clib::TCL_OK as c_int {
        return Err("expected integer Tcl_Obj".to_string());
    }
    Ok(out as i64)
}

unsafe fn parse_usize(
    interp: *mut clib::Tcl_Interp,
    obj: *mut clib::Tcl_Obj,
) -> Result<usize, String> {
    let value = parse_i64(interp, obj)?;
    usize::try_from(value).map_err(|_| "expected non-negative integer".to_string())
}

unsafe fn parse_u64(interp: *mut clib::Tcl_Interp, obj: *mut clib::Tcl_Obj) -> Result<u64, String> {
    let value = parse_i64(interp, obj)?;
    u64::try_from(value).map_err(|_| "expected non-negative integer".to_string())
}

unsafe fn obj_to_str<'a>(obj: *mut clib::Tcl_Obj) -> Result<&'a str, String> {
    let mut len: c_int = 0;
    let ptr = clib::Tcl_GetStringFromObj(obj, &mut len);
    if ptr.is_null() || len < 0 {
        return Err("invalid Tcl string object".to_string());
    }
    let bytes = slice::from_raw_parts(ptr as *const u8, len as usize);
    str::from_utf8(bytes).map_err(|_| "invalid utf-8 in Tcl object".to_string())
}

unsafe fn obj_to_bytes<'a>(obj: *mut clib::Tcl_Obj) -> Result<&'a [u8], String> {
    let mut len: c_int = 0;
    let ptr = clib::Tcl_GetStringFromObj(obj, &mut len);
    if ptr.is_null() || len < 0 {
        return Err("invalid Tcl byte/string object".to_string());
    }
    Ok(slice::from_raw_parts(ptr as *const u8, len as usize))
}

unsafe fn set_string_result(interp: *mut clib::Tcl_Interp, message: &str) {
    let ptr = message.as_bytes().as_ptr() as *const c_char;
    let obj = clib::Tcl_NewStringObj(ptr, message.len() as c_int);
    clib::Tcl_SetObjResult(interp, obj);
}

unsafe fn set_error(interp: *mut clib::Tcl_Interp, message: &str) -> c_int {
    let formatted = if message.starts_with("TCL_ERROR:") {
        message.to_string()
    } else {
        format!("TCL_ERROR: {}", message)
    };
    set_string_result(interp, formatted.as_str());
    clib::TCL_ERROR as c_int
}

unsafe fn set_ok_result(interp: *mut clib::Tcl_Interp, message: &str) -> c_int {
    set_string_result(interp, message);
    clib::TCL_OK as c_int
}

unsafe fn set_wide_result(interp: *mut clib::Tcl_Interp, value: i64) -> c_int {
    let obj = clib::Tcl_NewWideIntObj(value as clib::Tcl_WideInt);
    clib::Tcl_SetObjResult(interp, obj);
    clib::TCL_OK as c_int
}
