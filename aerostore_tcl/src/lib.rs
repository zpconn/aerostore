use std::os::raw::{c_char, c_int};
use std::path::PathBuf;
use std::ptr;
use std::slice;
use std::str;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use aerostore_core::{
    bulk_upsert_tsv, DurableDatabase, Field, IndexDefinition, IngestError, SortDirection,
    TsvColumns, TsvDecodeError, TsvDecoder,
};
use crossbeam::epoch;
use serde::{Deserialize, Serialize};
use tcl::Interp;

const PACKAGE_NAME: &str = "aerostore";
const PACKAGE_VERSION: &str = env!("CARGO_PKG_VERSION");
const DEFAULT_DATA_DIR: &str = "./aerostore_tcl_data";
const DEFAULT_BUCKET_COUNT: usize = 1 << 14;
const DEFAULT_CHECKPOINT_INTERVAL_SECS: u64 = 300;
const DEFAULT_INGEST_BATCH_SIZE: usize = 1024;

type FlightKey = String;
type FlightDb = DurableDatabase<FlightKey, FlightState>;

static GLOBAL_DB: OnceLock<Arc<FlightDb>> = OnceLock::new();
static GLOBAL_RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
static GLOBAL_DB_DIR: OnceLock<String> = OnceLock::new();

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FlightState {
    flight_id: String,
    lat: f64,
    lon: f64,
    altitude: i32,
    gs: u16,
    updated_at: u64,
    lat_scaled: i64,
    lon_scaled: i64,
}

impl FlightState {
    fn new(flight_id: String, lat: f64, lon: f64, altitude: i32, gs: u16, updated_at: u64) -> Self {
        Self {
            flight_id,
            lat,
            lon,
            altitude,
            gs,
            updated_at,
            lat_scaled: scale_coord(lat),
            lon_scaled: scale_coord(lon),
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

impl TsvDecoder<String, FlightState> for FlightTsvDecoder {
    fn decode(&self, cols: &mut TsvColumns<'_>) -> Result<(String, FlightState), TsvDecodeError> {
        let flight_id = cols.expect_str()?.to_owned();
        let lat = cols.expect_f64()?;
        let lon = cols.expect_f64()?;
        let altitude = cols.expect_i32()?;
        let gs = cols.expect_u16()?;
        let updated_at = cols.expect_u64()?;

        let row = FlightState::new(flight_id.clone(), lat, lon, altitude, gs, updated_at);

        Ok((flight_id, row))
    }
}

#[inline]
fn runtime() -> Result<&'static tokio::runtime::Runtime, String> {
    if let Some(rt) = GLOBAL_RT.get() {
        return Ok(rt);
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("aerostore-tcl")
        .build()
        .map_err(|err| format!("failed to create tokio runtime: {err}"))?;

    let _ = GLOBAL_RT.set(rt);
    GLOBAL_RT
        .get()
        .ok_or_else(|| "failed to initialize runtime".to_string())
}

#[inline]
fn default_indexes() -> Vec<IndexDefinition<FlightState>> {
    vec![
        IndexDefinition::new("flight_id", |row: &FlightState| row.flight_id.clone()),
        IndexDefinition::new("altitude", |row: &FlightState| row.altitude),
        IndexDefinition::new("gs", |row: &FlightState| row.gs),
        IndexDefinition::new("lat", |row: &FlightState| row.lat_scaled),
        IndexDefinition::new("lon", |row: &FlightState| row.lon_scaled),
        IndexDefinition::new("updated_at", |row: &FlightState| row.updated_at),
    ]
}

fn ensure_database(data_dir: Option<&str>) -> Result<&'static Arc<FlightDb>, String> {
    if let Some(db) = GLOBAL_DB.get() {
        return Ok(db);
    }

    let dir = data_dir.unwrap_or(DEFAULT_DATA_DIR);
    let rt = runtime()?;
    let dir_path = PathBuf::from(dir);

    let (db, _recovery) = rt
        .block_on(FlightDb::open_with_recovery(
            &dir_path,
            DEFAULT_BUCKET_COUNT,
            Duration::from_secs(DEFAULT_CHECKPOINT_INTERVAL_SECS),
            default_indexes(),
        ))
        .map_err(|err| format!("database open/recovery failed: {err}"))?;

    let _ = GLOBAL_DB_DIR.set(dir.to_string());
    let _ = GLOBAL_DB.set(Arc::new(db));
    GLOBAL_DB
        .get()
        .ok_or_else(|| "failed to initialize global database".to_string())
}

#[inline]
fn flight_id_field() -> Field<FlightState, String> {
    Field::new("flight_id", |row| row.flight_id.clone())
}

#[inline]
fn altitude_field() -> Field<FlightState, i32> {
    Field::new("altitude", |row| row.altitude)
}

#[inline]
fn gs_field() -> Field<FlightState, u16> {
    Field::new("gs", |row| row.gs)
}

#[inline]
fn lat_field() -> Field<FlightState, i64> {
    Field::new("lat", |row| row.lat_scaled)
}

#[inline]
fn lon_field() -> Field<FlightState, i64> {
    Field::new("lon", |row| row.lon_scaled)
}

#[inline]
fn updated_at_field() -> Field<FlightState, u64> {
    Field::new("updated_at", |row| row.updated_at)
}

#[inline]
fn scale_coord(value: f64) -> i64 {
    (value * 1_000_000.0).round() as i64
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
            "aerostore ready (dir={dir_msg}, txmgr={:p})",
            Arc::as_ptr(db)
        ),
    ))
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

    let tx = db.begin();
    let guard = epoch::pin();

    let mut query = db.engine().query();
    for comparison in options.comparisons {
        match apply_comparison(query, comparison) {
            Ok(next) => query = next,
            Err(err) => {
                db.abort(tx);
                return Err(err);
            }
        }
    }

    if let Some(sort_field) = options.sort_field {
        query = apply_sort(query, sort_field, options.sort_direction);
    }
    if let Some(limit) = options.limit {
        query = query.limit(limit);
    }
    if options.offset > 0 {
        query = query.offset(options.offset);
    }

    let rows = query.execute(tx.tx(), &guard);
    let count = rows.len() as i64;
    db.abort(tx);
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

    let decoder = FlightTsvDecoder;
    let rt = runtime()?;
    let stats = rt
        .block_on(bulk_upsert_tsv(db, tsv_data, batch_size, &decoder))
        .map_err(format_ingest_error)?;

    Ok(set_ok_result(interp, &stats.metrics_line()))
}

fn format_ingest_error(err: IngestError) -> String {
    match err {
        IngestError::EmptyBatchSize => "batch size must be > 0".to_string(),
        IngestError::Decode {
            line,
            column,
            message,
        } => format!("decode error at line {line} column {column}: {message}"),
        IngestError::Mvcc(mvcc) => format!("mvcc error: {mvcc:?}"),
        IngestError::Io(io) => format!("io error: {io}"),
    }
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

fn apply_comparison<'a>(
    query: aerostore_core::QueryBuilder<'a, String, FlightState>,
    comparison: Comparison,
) -> Result<aerostore_core::QueryBuilder<'a, String, FlightState>, String> {
    match comparison {
        Comparison::Eq(FieldName::FlightId, ComparisonValue::String(v)) => {
            Ok(query.eq(flight_id_field(), v))
        }
        Comparison::Eq(FieldName::Altitude, ComparisonValue::I32(v)) => {
            Ok(query.eq(altitude_field(), v))
        }
        Comparison::Eq(FieldName::Gs, ComparisonValue::U16(v)) => Ok(query.eq(gs_field(), v)),
        Comparison::Eq(FieldName::UpdatedAt, ComparisonValue::U64(v)) => {
            Ok(query.eq(updated_at_field(), v))
        }
        Comparison::Eq(FieldName::Lat, ComparisonValue::F64(v)) => {
            Ok(query.eq(lat_field(), scale_coord(v)))
        }
        Comparison::Eq(FieldName::Lon, ComparisonValue::F64(v)) => {
            Ok(query.eq(lon_field(), scale_coord(v)))
        }

        Comparison::Gt(FieldName::FlightId, ComparisonValue::String(v)) => {
            Ok(query.gt(flight_id_field(), v))
        }
        Comparison::Gt(FieldName::Altitude, ComparisonValue::I32(v)) => {
            Ok(query.gt(altitude_field(), v))
        }
        Comparison::Gt(FieldName::Gs, ComparisonValue::U16(v)) => Ok(query.gt(gs_field(), v)),
        Comparison::Gt(FieldName::UpdatedAt, ComparisonValue::U64(v)) => {
            Ok(query.gt(updated_at_field(), v))
        }
        Comparison::Gt(FieldName::Lat, ComparisonValue::F64(v)) => {
            Ok(query.gt(lat_field(), scale_coord(v)))
        }
        Comparison::Gt(FieldName::Lon, ComparisonValue::F64(v)) => {
            Ok(query.gt(lon_field(), scale_coord(v)))
        }

        Comparison::Lt(FieldName::FlightId, ComparisonValue::String(v)) => {
            Ok(query.lt(flight_id_field(), v))
        }
        Comparison::Lt(FieldName::Altitude, ComparisonValue::I32(v)) => {
            Ok(query.lt(altitude_field(), v))
        }
        Comparison::Lt(FieldName::Gs, ComparisonValue::U16(v)) => Ok(query.lt(gs_field(), v)),
        Comparison::Lt(FieldName::UpdatedAt, ComparisonValue::U64(v)) => {
            Ok(query.lt(updated_at_field(), v))
        }
        Comparison::Lt(FieldName::Lat, ComparisonValue::F64(v)) => {
            Ok(query.lt(lat_field(), scale_coord(v)))
        }
        Comparison::Lt(FieldName::Lon, ComparisonValue::F64(v)) => {
            Ok(query.lt(lon_field(), scale_coord(v)))
        }

        Comparison::In(FieldName::FlightId, values) => {
            let mut parsed = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    ComparisonValue::String(v) => parsed.push(v),
                    _ => return Err("flight_id IN values must all be strings".to_string()),
                }
            }
            Ok(query.in_values(flight_id_field(), parsed))
        }
        Comparison::In(FieldName::Altitude, values) => {
            let mut parsed = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    ComparisonValue::I32(v) => parsed.push(v),
                    _ => return Err("altitude IN values must all be integers".to_string()),
                }
            }
            Ok(query.in_values(altitude_field(), parsed))
        }
        Comparison::In(FieldName::Gs, values) => {
            let mut parsed = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    ComparisonValue::U16(v) => parsed.push(v),
                    _ => return Err("gs IN values must all be integers".to_string()),
                }
            }
            Ok(query.in_values(gs_field(), parsed))
        }
        Comparison::In(FieldName::UpdatedAt, values) => {
            let mut parsed = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    ComparisonValue::U64(v) => parsed.push(v),
                    _ => return Err("updated_at IN values must all be integers".to_string()),
                }
            }
            Ok(query.in_values(updated_at_field(), parsed))
        }
        Comparison::In(FieldName::Lat, values) => {
            let mut parsed = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    ComparisonValue::F64(v) => parsed.push(scale_coord(v)),
                    _ => return Err("lat IN values must all be doubles".to_string()),
                }
            }
            Ok(query.in_values(lat_field(), parsed))
        }
        Comparison::In(FieldName::Lon, values) => {
            let mut parsed = Vec::with_capacity(values.len());
            for value in values {
                match value {
                    ComparisonValue::F64(v) => parsed.push(scale_coord(v)),
                    _ => return Err("lon IN values must all be doubles".to_string()),
                }
            }
            Ok(query.in_values(lon_field(), parsed))
        }

        _ => Err("comparison field/value type mismatch".to_string()),
    }
}

fn apply_sort<'a>(
    query: aerostore_core::QueryBuilder<'a, String, FlightState>,
    field: FieldName,
    direction: SortDirection,
) -> aerostore_core::QueryBuilder<'a, String, FlightState> {
    match field {
        FieldName::FlightId => query.sort_by(flight_id_field(), direction),
        FieldName::Altitude => query.sort_by(altitude_field(), direction),
        FieldName::Gs => query.sort_by(gs_field(), direction),
        FieldName::UpdatedAt => query.sort_by(updated_at_field(), direction),
        FieldName::Lat => query.sort_by(lat_field(), direction),
        FieldName::Lon => query.sort_by(lon_field(), direction),
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
    set_string_result(interp, message);
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
