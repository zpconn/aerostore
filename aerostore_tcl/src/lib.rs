use std::cmp::Ordering;
use std::collections::HashMap;
use std::os::raw::{c_char, c_int};
use std::path::{Path, PathBuf};
use std::ptr;
use std::slice;
use std::str;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use aerostore_core::{
    spawn_wal_writer_daemon, IngestStats, IndexCatalog, IndexValue, OccError, OccTable,
    OccTransaction, PlannerError, QueryPlanner, SecondaryIndex, SharedWalRing, ShmArena,
    SortDirection, StapiFilter, StapiQuery, StapiRow, StapiValue, TsvColumns, TsvDecodeError,
    WalWriterError,
};
use crossbeam_skiplist::SkipMap;
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

type FlightDb = SharedFlightDb;

static GLOBAL_DB: OnceLock<Arc<FlightDb>> = OnceLock::new();
static GLOBAL_DB_DIR: OnceLock<String> = OnceLock::new();

#[derive(Clone, Copy, Debug, PartialEq)]
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
        let encoded_id = encode_fixed_ascii::<FLIGHT_ID_BYTES>(flight_id)
            .ok_or_else(|| format!("flight_id '{}' exceeds {} bytes", flight_id, FLIGHT_ID_BYTES))?;

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

#[derive(Clone, Copy, Debug)]
enum FieldName {
    FlightId,
    Altitude,
    Lat,
    Lon,
    Gs,
    UpdatedAt,
}

impl FieldName {
    fn parse(name: &str) -> Option<Self> {
        match name {
            "flight" | "flight_id" | "ident" => Some(Self::FlightId),
            "alt" | "altitude" => Some(Self::Altitude),
            "lat" => Some(Self::Lat),
            "lon" | "long" | "longitude" => Some(Self::Lon),
            "gs" | "groundspeed" => Some(Self::Gs),
            "updated_at" | "updated" | "ts" => Some(Self::UpdatedAt),
            _ => None,
        }
    }

    #[inline]
    fn canonical_name(self) -> &'static str {
        match self {
            FieldName::FlightId => "flight_id",
            FieldName::Altitude => "altitude",
            FieldName::Lat => "lat",
            FieldName::Lon => "lon",
            FieldName::Gs => "gs",
            FieldName::UpdatedAt => "updated_at",
        }
    }
}

#[derive(Clone, Debug)]
enum ComparisonValue {
    String(String),
    I32(i32),
    U16(u16),
    U64(u64),
    F64(f64),
}

#[derive(Clone, Debug)]
enum Comparison {
    Eq(FieldName, ComparisonValue),
    Gt(FieldName, ComparisonValue),
    Lt(FieldName, ComparisonValue),
    In(FieldName, Vec<ComparisonValue>),
    Match(FieldName, String),
}

struct SearchOptions {
    comparisons: Vec<Comparison>,
    sort_field: Option<FieldName>,
    sort_direction: SortDirection,
    limit: Option<usize>,
    offset: usize,
}

impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            comparisons: Vec::new(),
            sort_field: None,
            sort_direction: SortDirection::Asc,
            limit: None,
            offset: 0,
        }
    }
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
struct FlightIndexes {
    flight_id: Arc<SecondaryIndex<usize>>,
    altitude: Arc<SecondaryIndex<usize>>,
    gs: Arc<SecondaryIndex<usize>>,
    lat: Arc<SecondaryIndex<usize>>,
    lon: Arc<SecondaryIndex<usize>>,
    updated_at: Arc<SecondaryIndex<usize>>,
}

impl FlightIndexes {
    fn new() -> Self {
        Self {
            flight_id: Arc::new(SecondaryIndex::new("flight_id")),
            altitude: Arc::new(SecondaryIndex::new("altitude")),
            gs: Arc::new(SecondaryIndex::new("gs")),
            lat: Arc::new(SecondaryIndex::new("lat")),
            lon: Arc::new(SecondaryIndex::new("lon")),
            updated_at: Arc::new(SecondaryIndex::new("updated_at")),
        }
    }

    fn as_catalog(&self) -> IndexCatalog {
        let mut catalog = IndexCatalog::new();

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

        catalog
    }

    fn remove_row(&self, row_id: usize, row: &FlightState) {
        if row.exists == 0 {
            return;
        }

        let row_key = row.flight_id_string();
        self.flight_id
            .remove(&IndexValue::String(row_key), &row_id);

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
        self.flight_id
            .insert(IndexValue::String(row_key), row_id);

        self.altitude
            .insert(IndexValue::I64(row.altitude as i64), row_id);
        self.gs.insert(IndexValue::I64(row.gs as i64), row_id);
        self.lat.insert(IndexValue::I64(row.lat_scaled), row_id);
        self.lon.insert(IndexValue::I64(row.lon_scaled), row_id);
        if let Ok(updated_at) = i64::try_from(row.updated_at) {
            self.updated_at
                .insert(IndexValue::I64(updated_at), row_id);
        }
    }
}

struct SharedFlightDb {
    table: Arc<OccTable<FlightState>>,
    planner: QueryPlanner<FlightState>,
    key_index: SkipMap<String, usize>,
    next_row_id: AtomicUsize,
    row_capacity: usize,
    indexes: FlightIndexes,
    committer: Mutex<aerostore_core::OccCommitter<WAL_RING_SLOTS, WAL_RING_SLOT_BYTES>>,
    _ring: SharedWalRing<WAL_RING_SLOTS, WAL_RING_SLOT_BYTES>,
    _wal_daemon: aerostore_core::WalWriterDaemon,
    _data_dir: PathBuf,
}

impl SharedFlightDb {
    fn open(data_dir: &Path) -> Result<Self, String> {
        std::fs::create_dir_all(data_dir)
            .map_err(|err| format!("failed to create data dir '{}': {}", data_dir.display(), err))?;

        let shm = Arc::new(
            ShmArena::new(DEFAULT_SHM_ARENA_BYTES)
                .map_err(|err| format!("failed to allocate shared memory: {}", err))?,
        );
        let table = Arc::new(
            OccTable::<FlightState>::new(Arc::clone(&shm), DEFAULT_ROW_CAPACITY)
                .map_err(|err| format!("failed to create OCC table: {}", err))?,
        );

        for row_id in 0..DEFAULT_ROW_CAPACITY {
            table
                .seed_row(row_id, FlightState::empty())
                .map_err(|err| format!("failed to seed OCC row {}: {}", row_id, err))?;
        }

        let indexes = FlightIndexes::new();
        let planner = QueryPlanner::new(indexes.as_catalog());

        let ring = SharedWalRing::<WAL_RING_SLOTS, WAL_RING_SLOT_BYTES>::create(Arc::clone(&shm))
            .map_err(|err| format!("failed to create shared WAL ring: {}", err))?;
        let wal_path = data_dir.join(WAL_FILE_NAME);
        let wal_daemon = spawn_wal_writer_daemon(ring.clone(), &wal_path)
            .map_err(|err| format!("failed to spawn WAL writer daemon: {}", err))?;
        let committer = aerostore_core::OccCommitter::<WAL_RING_SLOTS, WAL_RING_SLOT_BYTES>::new_asynchronous(
            ring.clone(),
        );

        Ok(Self {
            table,
            planner,
            key_index: SkipMap::new(),
            next_row_id: AtomicUsize::new(0),
            row_capacity: DEFAULT_ROW_CAPACITY,
            indexes,
            committer: Mutex::new(committer),
            _ring: ring,
            _wal_daemon: wal_daemon,
            _data_dir: data_dir.to_path_buf(),
        })
    }

    fn search_count(&self, options: SearchOptions) -> Result<usize, String> {
        let query = options_to_stapi_query(&options)?;
        let plan = self
            .planner
            .compile(&query)
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

        if let Some(sort_field) = options.sort_field {
            rows.sort_unstable_by(|left, right| compare_rows(sort_field, left, right));
            if matches!(options.sort_direction, SortDirection::Desc) {
                rows.reverse();
            }
        }

        let start = options.offset.min(rows.len());
        let end = options
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

        let mut tx = self
            .table
            .begin_transaction()
            .map_err(|err| format!("begin_transaction failed: {}", err))?;

        let mut pending_index_updates: HashMap<usize, PendingIndexUpdate> = HashMap::new();
        let mut batch_ops = 0_usize;

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
            self.upsert_one(
                &mut tx,
                &mut pending_index_updates,
                flight_key,
                row,
                &mut stats,
            )?;
            batch_ops += 1;

            if batch_ops >= batch_size {
                self.commit_batch(&mut tx, &mut pending_index_updates)?;
                stats.batches_committed += 1;
                tx = self
                    .table
                    .begin_transaction()
                    .map_err(|err| format!("begin_transaction failed: {}", err))?;
                batch_ops = 0;
            }
        }

        if batch_ops > 0 {
            self.commit_batch(&mut tx, &mut pending_index_updates)?;
            stats.batches_committed += 1;
        } else {
            self.table
                .abort(&mut tx)
                .map_err(|err| format!("abort failed: {}", err))?;
        }

        stats.elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        Ok(stats)
    }

    fn upsert_one(
        &self,
        tx: &mut OccTransaction<FlightState>,
        pending_index_updates: &mut HashMap<usize, PendingIndexUpdate>,
        flight_key: String,
        next_row: FlightState,
        stats: &mut IngestStats,
    ) -> Result<(), String> {
        let row_id = self.resolve_or_allocate_row_id(flight_key.as_str())?;
        let current = self
            .table
            .read(tx, row_id)
            .map_err(|err| format!("read failed for row {}: {}", row_id, err))?
            .ok_or_else(|| format!("row {} is unexpectedly missing", row_id))?;

        let was_live = current.exists != 0;

        self.table
            .write(tx, row_id, next_row)
            .map_err(|err| format!("write failed for row {}: {}", row_id, err))?;

        pending_index_updates
            .entry(row_id)
            .and_modify(|update| update.after = next_row)
            .or_insert(PendingIndexUpdate {
                before: current,
                after: next_row,
            });

        if was_live {
            stats.rows_updated += 1;
        } else {
            stats.rows_inserted += 1;
        }

        Ok(())
    }

    fn resolve_or_allocate_row_id(&self, flight_key: &str) -> Result<usize, String> {
        if let Some(entry) = self.key_index.get(flight_key) {
            return Ok(*entry.value());
        }

        let candidate = self.next_row_id.fetch_add(1, AtomicOrdering::AcqRel);
        if candidate >= self.row_capacity {
            return Err(format!(
                "row capacity exhausted (capacity={}, key='{}')",
                self.row_capacity, flight_key
            ));
        }

        let entry = self
            .key_index
            .get_or_insert(flight_key.to_string(), candidate);
        let row_id = *entry.value();

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
    ) -> Result<(), String> {
        let commit_result = {
            let mut guard = self
                .committer
                .lock()
                .map_err(|_| "committer lock poisoned".to_string())?;
            guard.commit(&self.table, tx)
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
                Err("serialization failure during OCC commit; retry the operation".to_string())
            }
            Err(err) => {
                pending_index_updates.clear();
                Err(format!("commit failed: {}", err))
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

fn compare_rows(field: FieldName, left: &FlightState, right: &FlightState) -> Ordering {
    match field {
        FieldName::FlightId => left.flight_id.cmp(&right.flight_id),
        FieldName::Altitude => left.altitude.cmp(&right.altitude),
        FieldName::Lat => left.lat_scaled.cmp(&right.lat_scaled),
        FieldName::Lon => left.lon_scaled.cmp(&right.lon_scaled),
        FieldName::Gs => left.gs.cmp(&right.gs),
        FieldName::UpdatedAt => left.updated_at.cmp(&right.updated_at),
    }
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

fn options_to_stapi_query(options: &SearchOptions) -> Result<StapiQuery, String> {
    let mut filters = Vec::with_capacity(options.comparisons.len());

    for comparison in &options.comparisons {
        filters.push(comparison_to_stapi_filter(comparison)?);
    }

    Ok(StapiQuery {
        filters,
        sort: None,
        limit: None,
    })
}

fn comparison_to_stapi_filter(comparison: &Comparison) -> Result<StapiFilter, String> {
    match comparison {
        Comparison::Eq(field, value) => Ok(StapiFilter::Eq {
            field: field.canonical_name().to_string(),
            value: comparison_value_to_stapi(*field, value)?,
        }),
        Comparison::Gt(field, value) => Ok(StapiFilter::Gt {
            field: field.canonical_name().to_string(),
            value: comparison_value_to_stapi(*field, value)?,
        }),
        Comparison::Lt(field, value) => Ok(StapiFilter::Lt {
            field: field.canonical_name().to_string(),
            value: comparison_value_to_stapi(*field, value)?,
        }),
        Comparison::In(field, values) => {
            let mut mapped = Vec::with_capacity(values.len());
            for value in values {
                mapped.push(comparison_value_to_stapi(*field, value)?);
            }
            Ok(StapiFilter::In {
                field: field.canonical_name().to_string(),
                values: mapped,
            })
        }
        Comparison::Match(field, pattern) => Ok(StapiFilter::Match {
            field: field.canonical_name().to_string(),
            pattern: pattern.clone(),
        }),
    }
}

fn comparison_value_to_stapi(
    field: FieldName,
    value: &ComparisonValue,
) -> Result<StapiValue, String> {
    match (field, value) {
        (FieldName::FlightId, ComparisonValue::String(v)) => Ok(StapiValue::Text(v.clone())),
        (FieldName::Altitude, ComparisonValue::I32(v)) => Ok(StapiValue::Int(*v as i64)),
        (FieldName::Gs, ComparisonValue::U16(v)) => Ok(StapiValue::Int(*v as i64)),
        (FieldName::UpdatedAt, ComparisonValue::U64(v)) => {
            let mapped = i64::try_from(*v)
                .map_err(|_| "updated_at exceeds signed 64-bit range".to_string())?;
            Ok(StapiValue::Int(mapped))
        }
        (FieldName::Lat, ComparisonValue::F64(v)) => Ok(StapiValue::Int(scale_coord(*v))),
        (FieldName::Lon, ComparisonValue::F64(v)) => Ok(StapiValue::Int(scale_coord(*v))),
        _ => Err("comparison field/value type mismatch".to_string()),
    }
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

    Ok(set_ok_result(
        interp,
        &format!(
            "aerostore ready (dir={dir_msg}, occ_table={:p})",
            Arc::as_ptr(db)
        ),
    ))
}

fn ensure_database(data_dir: Option<&str>) -> Result<&'static Arc<FlightDb>, String> {
    if let Some(db) = GLOBAL_DB.get() {
        return Ok(db);
    }

    let dir = data_dir.unwrap_or(DEFAULT_DATA_DIR);
    let db = SharedFlightDb::open(Path::new(dir))?;

    let _ = GLOBAL_DB_DIR.set(dir.to_string());
    let _ = GLOBAL_DB.set(Arc::new(db));
    GLOBAL_DB
        .get()
        .ok_or_else(|| "failed to initialize global database".to_string())
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
    let options = parse_search_options(interp, args)?;
    let db = ensure_database(None)?;
    let count = db.search_count(options)? as i64;
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

unsafe fn parse_search_options(
    interp: *mut clib::Tcl_Interp,
    args: &[*mut clib::Tcl_Obj],
) -> Result<SearchOptions, String> {
    let mut opts = SearchOptions {
        sort_direction: SortDirection::Asc,
        ..SearchOptions::default()
    };

    let mut i = 2_usize;
    while i < args.len() {
        let opt = obj_to_str(args[i])?;
        match opt {
            "-compare" => {
                if i + 1 >= args.len() {
                    return Err("missing value for -compare".to_string());
                }
                let mut parsed = parse_compare_list(interp, args[i + 1])?;
                opts.comparisons.append(&mut parsed);
                i += 2;
            }
            "-sort" => {
                if i + 1 >= args.len() {
                    return Err("missing value for -sort".to_string());
                }
                let field_str = obj_to_str(args[i + 1])?;
                let field = FieldName::parse(field_str)
                    .ok_or_else(|| format!("unknown sort field: {field_str}"))?;
                opts.sort_field = Some(field);
                i += 2;
            }
            "-limit" => {
                if i + 1 >= args.len() {
                    return Err("missing value for -limit".to_string());
                }
                opts.limit = Some(parse_usize(interp, args[i + 1])?);
                i += 2;
            }
            "-offset" => {
                if i + 1 >= args.len() {
                    return Err("missing value for -offset".to_string());
                }
                opts.offset = parse_usize(interp, args[i + 1])?;
                i += 2;
            }
            "-desc" => {
                opts.sort_direction = SortDirection::Desc;
                i += 1;
            }
            "-asc" => {
                opts.sort_direction = SortDirection::Asc;
                i += 1;
            }
            _ => return Err(format!("unknown search option: {opt}")),
        }
    }

    Ok(opts)
}

unsafe fn parse_compare_list(
    interp: *mut clib::Tcl_Interp,
    compare_obj: *mut clib::Tcl_Obj,
) -> Result<Vec<Comparison>, String> {
    let items = list_elements(interp, compare_obj)
        .map_err(|_| "expected -compare to be a Tcl list".to_string())?;
    let mut out = Vec::with_capacity(items.len());

    for item in items {
        let triplet = list_elements(interp, item)
            .map_err(|_| "each compare clause must be a list: {op field value}".to_string())?;
        if triplet.len() != 3 {
            return Err("compare clause must have exactly 3 elements".to_string());
        }

        let op = obj_to_str(triplet[0])?;
        let field_name = obj_to_str(triplet[1])?;
        let field = FieldName::parse(field_name)
            .ok_or_else(|| format!("unknown compare field: {field_name}"))?;

        let comparison = match op {
            "=" | "==" => Comparison::Eq(field, parse_value(interp, field, triplet[2])?),
            ">" => Comparison::Gt(field, parse_value(interp, field, triplet[2])?),
            "<" => Comparison::Lt(field, parse_value(interp, field, triplet[2])?),
            "in" | "IN" => {
                let values = list_elements(interp, triplet[2]).map_err(|_| {
                    "operator 'in' expects a Tcl list of values as the third item".to_string()
                })?;
                let mut parsed_values = Vec::with_capacity(values.len());
                for value in values {
                    parsed_values.push(parse_value(interp, field, value)?);
                }
                Comparison::In(field, parsed_values)
            }
            "match" => {
                if !matches!(field, FieldName::FlightId) {
                    return Err("operator 'match' currently supports only flight_id fields".to_string());
                }
                Comparison::Match(field, obj_to_str(triplet[2])?.to_string())
            }
            _ => return Err(format!("unsupported compare operator: {op}")),
        };

        out.push(comparison);
    }

    Ok(out)
}

unsafe fn parse_value(
    interp: *mut clib::Tcl_Interp,
    field: FieldName,
    value_obj: *mut clib::Tcl_Obj,
) -> Result<ComparisonValue, String> {
    match field {
        FieldName::FlightId => Ok(ComparisonValue::String(obj_to_str(value_obj)?.to_owned())),
        FieldName::Altitude => {
            let v = parse_i64(interp, value_obj)?;
            let v = i32::try_from(v).map_err(|_| "altitude must fit i32".to_string())?;
            Ok(ComparisonValue::I32(v))
        }
        FieldName::Gs => {
            let v = parse_i64(interp, value_obj)?;
            let v = u16::try_from(v).map_err(|_| "groundspeed must fit u16".to_string())?;
            Ok(ComparisonValue::U16(v))
        }
        FieldName::UpdatedAt => {
            let v = parse_i64(interp, value_obj)?;
            let v = u64::try_from(v).map_err(|_| "updated_at must be >= 0".to_string())?;
            Ok(ComparisonValue::U64(v))
        }
        FieldName::Lat | FieldName::Lon => Ok(ComparisonValue::F64(parse_f64(interp, value_obj)?)),
    }
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

unsafe fn list_elements(
    interp: *mut clib::Tcl_Interp,
    list_obj: *mut clib::Tcl_Obj,
) -> Result<Vec<*mut clib::Tcl_Obj>, ()> {
    let mut objc: c_int = 0;
    let mut objv: *mut *mut clib::Tcl_Obj = ptr::null_mut();
    let rc = clib::Tcl_ListObjGetElements(interp, list_obj, &mut objc, &mut objv);
    if rc != clib::TCL_OK as c_int || objc < 0 || objv.is_null() {
        return Err(());
    }
    Ok(slice::from_raw_parts(objv, objc as usize).to_vec())
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

unsafe fn parse_f64(interp: *mut clib::Tcl_Interp, obj: *mut clib::Tcl_Obj) -> Result<f64, String> {
    let mut out = 0.0_f64;
    let rc = clib::Tcl_GetDoubleFromObj(interp, obj, &mut out);
    if rc != clib::TCL_OK as c_int {
        return Err("expected double Tcl_Obj".to_string());
    }
    Ok(out)
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
