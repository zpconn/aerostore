use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::index::{IndexCompare, IndexValue, SecondaryIndex};
use crate::occ::{Error as OccError, OccTable, OccTransaction};
use crate::stapi_parser::{parse_stapi_query, Filter, ParseError, Query, Value};

type RowPredicate<T> = Arc<dyn Fn(&T) -> bool + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanRoute {
    Indexed,
    FullScan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannerError {
    Parse(String),
    UnknownField(String),
    InvalidIndexedValue(String),
    Occ(String),
}

impl PlannerError {
    #[inline]
    pub fn tcl_error_message(&self) -> String {
        format!("TCL_ERROR: {}", self)
    }
}

impl fmt::Display for PlannerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlannerError::Parse(msg) => write!(f, "parse error: {}", msg),
            PlannerError::UnknownField(field) => write!(f, "unknown field '{}'", field),
            PlannerError::InvalidIndexedValue(msg) => write!(f, "{}", msg),
            PlannerError::Occ(msg) => write!(f, "occ error: {}", msg),
        }
    }
}

impl std::error::Error for PlannerError {}

impl From<ParseError> for PlannerError {
    fn from(value: ParseError) -> Self {
        PlannerError::Parse(value.message().to_string())
    }
}

impl From<OccError> for PlannerError {
    fn from(value: OccError) -> Self {
        PlannerError::Occ(value.to_string())
    }
}

pub trait StapiRow: Copy + Send + Sync + 'static {
    fn has_field(field: &str) -> bool;
    fn field_value(&self, field: &str) -> Option<Value>;
}

#[derive(Clone, Default)]
pub struct IndexCatalog {
    indexes: HashMap<String, Arc<SecondaryIndex<usize>>>,
}

impl IndexCatalog {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn with_index(
        mut self,
        field: impl Into<String>,
        index: Arc<SecondaryIndex<usize>>,
    ) -> Self {
        self.insert_index(field, index);
        self
    }

    #[inline]
    pub fn insert_index(&mut self, field: impl Into<String>, index: Arc<SecondaryIndex<usize>>) {
        self.indexes.insert(field.into(), index);
    }

    #[inline]
    fn contains(&self, field: &str) -> bool {
        self.indexes.contains_key(field)
    }

    #[inline]
    fn get(&self, field: &str) -> Option<&Arc<SecondaryIndex<usize>>> {
        self.indexes.get(field)
    }
}

enum RoutingPlan {
    Indexed {
        field: String,
        compare: IndexCompare,
    },
    FullScan,
}

pub struct ExecutionPlan<T: StapiRow> {
    route: RoutingPlan,
    filters: Vec<RowPredicate<T>>,
    sort: Option<String>,
    limit: Option<usize>,
    indexes: IndexCatalog,
}

impl<T: StapiRow> ExecutionPlan<T> {
    #[inline]
    pub fn route(&self) -> PlanRoute {
        match self.route {
            RoutingPlan::Indexed { .. } => PlanRoute::Indexed,
            RoutingPlan::FullScan => PlanRoute::FullScan,
        }
    }

    pub fn execute(
        &self,
        table: &OccTable<T>,
        tx: &mut OccTransaction<T>,
    ) -> Result<Vec<T>, PlannerError> {
        let mut rows = Vec::new();
        let candidate_row_ids = self.candidate_row_ids(table);

        for row_id in candidate_row_ids {
            let Some(row) = table.read(tx, row_id)? else {
                continue;
            };

            if self.filters.iter().all(|predicate| predicate(&row)) {
                rows.push(row);
            }
        }

        if let Some(sort_field) = &self.sort {
            rows.sort_unstable_by(|left, right| {
                compare_optional(
                    left.field_value(sort_field.as_str()),
                    right.field_value(sort_field.as_str()),
                )
            });
        }

        if let Some(limit) = self.limit {
            rows.truncate(limit);
        }

        Ok(rows)
    }

    fn candidate_row_ids(&self, table: &OccTable<T>) -> Vec<usize> {
        match &self.route {
            RoutingPlan::Indexed { field, compare } => self
                .indexes
                .get(field.as_str())
                .map(|index| index.lookup(compare))
                .unwrap_or_else(|| (0..table.capacity()).collect()),
            RoutingPlan::FullScan => (0..table.capacity()).collect(),
        }
    }
}

pub struct QueryPlanner<T: StapiRow> {
    indexes: IndexCatalog,
    _marker: PhantomData<T>,
}

impl<T: StapiRow> QueryPlanner<T> {
    #[inline]
    pub fn new(indexes: IndexCatalog) -> Self {
        Self {
            indexes,
            _marker: PhantomData,
        }
    }

    pub fn compile_from_stapi(&self, stapi: &str) -> Result<ExecutionPlan<T>, PlannerError> {
        let query = parse_stapi_query(stapi)?;
        self.compile(&query)
    }

    pub fn compile(&self, query: &Query) -> Result<ExecutionPlan<T>, PlannerError> {
        for filter in &query.filters {
            let field = field_name(filter);
            if !T::has_field(field) {
                return Err(PlannerError::UnknownField(field.to_string()));
            }
        }

        if let Some(sort) = &query.sort {
            if !T::has_field(sort.as_str()) {
                return Err(PlannerError::UnknownField(sort.clone()));
            }
        }

        let route = self.select_route(query)?;
        let mut compiled_filters = Vec::with_capacity(query.filters.len());
        for filter in &query.filters {
            compiled_filters.push(compile_filter::<T>(filter));
        }

        Ok(ExecutionPlan {
            route,
            filters: compiled_filters,
            sort: query.sort.clone(),
            limit: query.limit,
            indexes: self.indexes.clone(),
        })
    }

    fn select_route(&self, query: &Query) -> Result<RoutingPlan, PlannerError> {
        for filter in &query.filters {
            match filter {
                Filter::Eq { field, value } => {
                    if self.indexes.contains(field.as_str()) {
                        let indexed_value = value_to_index(value).ok_or_else(|| {
                            PlannerError::InvalidIndexedValue(format!(
                                "field '{}' cannot use this value in an indexed equality route",
                                field
                            ))
                        })?;
                        return Ok(RoutingPlan::Indexed {
                            field: field.clone(),
                            compare: IndexCompare::Eq(indexed_value),
                        });
                    }
                }
                Filter::Gt { field, value } => {
                    if self.indexes.contains(field.as_str()) {
                        let indexed_value = value_to_index(value).ok_or_else(|| {
                            PlannerError::InvalidIndexedValue(format!(
                                "field '{}' cannot use this value in an indexed range route",
                                field
                            ))
                        })?;
                        return Ok(RoutingPlan::Indexed {
                            field: field.clone(),
                            compare: IndexCompare::Gt(indexed_value),
                        });
                    }
                }
                _ => {}
            }
        }

        Ok(RoutingPlan::FullScan)
    }
}

fn field_name(filter: &Filter) -> &str {
    match filter {
        Filter::Eq { field, .. } => field.as_str(),
        Filter::Lt { field, .. } => field.as_str(),
        Filter::Gt { field, .. } => field.as_str(),
        Filter::In { field, .. } => field.as_str(),
        Filter::Match { field, .. } => field.as_str(),
    }
}

fn compile_filter<T: StapiRow>(filter: &Filter) -> RowPredicate<T> {
    match filter {
        Filter::Eq { field, value } => {
            let field = field.clone();
            let expected = value.clone();
            Arc::new(move |row: &T| {
                row.field_value(field.as_str())
                    .map(|actual| values_equal(&actual, &expected))
                    .unwrap_or(false)
            })
        }
        Filter::Lt { field, value } => {
            let field = field.clone();
            let expected = value.clone();
            Arc::new(move |row: &T| {
                row.field_value(field.as_str())
                    .map(|actual| {
                        matches!(compare_values(&actual, &expected), Some(Ordering::Less))
                    })
                    .unwrap_or(false)
            })
        }
        Filter::Gt { field, value } => {
            let field = field.clone();
            let expected = value.clone();
            Arc::new(move |row: &T| {
                row.field_value(field.as_str())
                    .map(|actual| {
                        matches!(compare_values(&actual, &expected), Some(Ordering::Greater))
                    })
                    .unwrap_or(false)
            })
        }
        Filter::In { field, values } => {
            let field = field.clone();
            let options = values.clone();
            Arc::new(move |row: &T| {
                let Some(actual) = row.field_value(field.as_str()) else {
                    return false;
                };
                options
                    .iter()
                    .any(|expected| values_equal(&actual, expected))
            })
        }
        Filter::Match { field, pattern } => {
            let field = field.clone();
            let pattern = pattern.clone();
            Arc::new(move |row: &T| {
                let Some(Value::Text(actual)) = row.field_value(field.as_str()) else {
                    return false;
                };
                glob_matches(pattern.as_str(), actual.as_str())
            })
        }
    }
}

fn values_equal(left: &Value, right: &Value) -> bool {
    matches!(compare_values(left, right), Some(Ordering::Equal))
}

fn compare_optional(left: Option<Value>, right: Option<Value>) -> Ordering {
    match (left, right) {
        (Some(lhs), Some(rhs)) => compare_values(&lhs, &rhs).unwrap_or(Ordering::Equal),
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (None, None) => Ordering::Equal,
    }
}

fn compare_values(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Int(lhs), Value::Int(rhs)) => Some(lhs.cmp(rhs)),
        (Value::Float(lhs), Value::Float(rhs)) => lhs.partial_cmp(rhs),
        (Value::Int(lhs), Value::Float(rhs)) => (*lhs as f64).partial_cmp(rhs),
        (Value::Float(lhs), Value::Int(rhs)) => lhs.partial_cmp(&(*rhs as f64)),
        (Value::Text(lhs), Value::Text(rhs)) => Some(lhs.cmp(rhs)),
        _ => None,
    }
}

fn value_to_index(value: &Value) -> Option<IndexValue> {
    match value {
        Value::Int(v) => Some(IndexValue::I64(*v)),
        Value::Text(v) => Some(IndexValue::String(v.clone())),
        Value::Float(v) => {
            if !v.is_finite() || v.fract() != 0.0 {
                return None;
            }
            if *v < i64::MIN as f64 || *v > i64::MAX as f64 {
                return None;
            }
            Some(IndexValue::I64(*v as i64))
        }
    }
}

fn glob_matches(pattern: &str, value: &str) -> bool {
    let pattern = pattern.as_bytes();
    let value = value.as_bytes();

    let mut p_idx = 0_usize;
    let mut v_idx = 0_usize;
    let mut star_idx: Option<usize> = None;
    let mut fallback_v_idx = 0_usize;

    while v_idx < value.len() {
        if p_idx < pattern.len() && (pattern[p_idx] == b'?' || pattern[p_idx] == value[v_idx]) {
            p_idx += 1;
            v_idx += 1;
            continue;
        }

        if p_idx < pattern.len() && pattern[p_idx] == b'*' {
            star_idx = Some(p_idx);
            p_idx += 1;
            fallback_v_idx = v_idx;
            continue;
        }

        if let Some(star) = star_idx {
            p_idx = star + 1;
            fallback_v_idx += 1;
            v_idx = fallback_v_idx;
            continue;
        }

        return false;
    }

    while p_idx < pattern.len() && pattern[p_idx] == b'*' {
        p_idx += 1;
    }

    p_idx == pattern.len()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{IndexCatalog, PlanRoute, QueryPlanner, StapiRow};
    use crate::index::{IndexValue, SecondaryIndex};
    use crate::occ::{Error as OccError, OccTable};
    use crate::shm::ShmArena;
    use crate::stapi_parser::{parse_stapi_query, Value};

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct FlightRow {
        alt: i64,
        flight: [u8; 8],
        typ: [u8; 4],
    }

    impl FlightRow {
        fn new(alt: i64, flight: &str, typ: &str) -> Self {
            Self {
                alt,
                flight: fixed_ascii::<8>(flight),
                typ: fixed_ascii::<4>(typ),
            }
        }
    }

    impl StapiRow for FlightRow {
        fn has_field(field: &str) -> bool {
            matches!(field, "alt" | "flight" | "typ")
        }

        fn field_value(&self, field: &str) -> Option<Value> {
            match field {
                "alt" => Some(Value::Int(self.alt)),
                "flight" => Some(Value::Text(decode_ascii(&self.flight))),
                "typ" => Some(Value::Text(decode_ascii(&self.typ))),
                _ => None,
            }
        }
    }

    fn fixed_ascii<const N: usize>(value: &str) -> [u8; N] {
        let mut out = [0_u8; N];
        let bytes = value.as_bytes();
        let len = bytes.len().min(N);
        out[..len].copy_from_slice(&bytes[..len]);
        out
    }

    fn decode_ascii(bytes: &[u8]) -> String {
        let end = bytes.iter().position(|v| *v == 0).unwrap_or(bytes.len());
        String::from_utf8_lossy(&bytes[..end]).to_string()
    }

    fn seeded_occ_with_alt_index() -> (OccTable<FlightRow>, IndexCatalog) {
        let shm = Arc::new(ShmArena::new(16 << 20).expect("failed to allocate shared arena"));
        let table =
            OccTable::<FlightRow>::new(Arc::clone(&shm), 8).expect("failed to create occ table");

        let rows = [
            FlightRow::new(12_000, "UAL123", "B738"),
            FlightRow::new(15_000, "UAL987", "A320"),
            FlightRow::new(8_000, "DAL111", "B738"),
            FlightRow::new(35_000, "UAL555", "B77W"),
        ];

        let alt_index = Arc::new(SecondaryIndex::<usize>::new_in_shared(
            "alt",
            Arc::clone(&shm),
        ));
        for (row_id, row) in rows.into_iter().enumerate() {
            table
                .seed_row(row_id, row)
                .expect("failed to seed OCC test row");
            alt_index.insert(IndexValue::I64(row.alt), row_id);
        }

        let catalog = IndexCatalog::new().with_index("alt", alt_index);
        (table, catalog)
    }

    #[test]
    fn compiles_and_executes_nested_stapi_plan_with_index_route() {
        let (table, indexes) = seeded_occ_with_alt_index();
        let planner = QueryPlanner::<FlightRow>::new(indexes);
        let query = parse_stapi_query(
            "-compare {{match flight UAL*} {> alt 10000} {in typ {B738 A320}}} -sort alt -limit 50",
        )
        .expect("failed to parse STAPI query");

        let plan = planner.compile(&query).expect("failed to compile plan");
        assert_eq!(plan.route(), PlanRoute::Indexed);

        let mut tx = table.begin_transaction().expect("begin_transaction failed");
        let rows = plan
            .execute(&table, &mut tx)
            .expect("plan execution should succeed");
        table.abort(&mut tx).expect("abort failed");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].alt, 12_000);
        assert_eq!(rows[1].alt, 15_000);
        assert_eq!(decode_ascii(&rows[0].flight), "UAL123");
        assert_eq!(decode_ascii(&rows[1].flight), "UAL987");
    }

    #[test]
    fn compiles_supported_operators_correctly() {
        let (table, indexes) = seeded_occ_with_alt_index();
        let planner = QueryPlanner::<FlightRow>::new(indexes);
        let query = parse_stapi_query(
            "-compare {{= typ B738} {< alt 13000} {> alt 10000} {in typ {B738 A320}} {match flight UAL*}}",
        )
        .expect("failed to parse operator query");

        let plan = planner.compile(&query).expect("failed to compile plan");
        assert_eq!(plan.route(), PlanRoute::Indexed);

        let mut tx = table.begin_transaction().expect("begin_transaction failed");
        let rows = plan
            .execute(&table, &mut tx)
            .expect("plan execution should succeed");
        table.abort(&mut tx).expect("abort failed");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].alt, 12_000);
        assert_eq!(decode_ascii(&rows[0].flight), "UAL123");
    }

    #[test]
    fn planner_rejects_unknown_fields_with_explicit_tcl_error() {
        let (_table, indexes) = seeded_occ_with_alt_index();
        let planner = QueryPlanner::<FlightRow>::new(indexes);
        let parsed = parse_stapi_query("-compare {{> unknown_field 1}}")
            .expect("parser should allow opaque field tokens");

        let err = match planner.compile(&parsed) {
            Ok(_) => panic!("planner should reject unknown fields"),
            Err(err) => err,
        };
        let tcl_message = err.tcl_error_message();
        assert!(tcl_message.starts_with("TCL_ERROR:"));
        assert!(tcl_message.contains("unknown field"));
    }

    #[test]
    fn malformed_stapi_is_reported_without_panicking_boundary() {
        let (_table, indexes) = seeded_occ_with_alt_index();
        let planner = QueryPlanner::<FlightRow>::new(indexes);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            planner
                .compile_from_stapi("-compare {{> alt 10000}")
                .map_err(|err| err.tcl_error_message())
        }));

        assert!(result.is_ok(), "planner boundary must not panic");
        let inner = result.expect("planner should not panic");
        assert!(inner.is_err(), "malformed query must return an error");
        let err = match inner {
            Ok(_) => panic!("expected planner error"),
            Err(err) => err,
        };
        assert!(err.starts_with("TCL_ERROR:"));
    }

    #[test]
    fn planner_reads_are_tracked_for_ssi_validation() {
        let (table, indexes) = seeded_occ_with_alt_index();
        let planner = QueryPlanner::<FlightRow>::new(indexes);
        let parsed =
            parse_stapi_query("-compare {{> alt 10000}}").expect("failed to parse read query");
        let plan = planner
            .compile(&parsed)
            .expect("failed to compile read plan");
        assert_eq!(plan.route(), PlanRoute::Indexed);

        let mut tx_a = table
            .begin_transaction()
            .expect("tx_a begin_transaction failed");
        let observed_rows = plan
            .execute(&table, &mut tx_a)
            .expect("tx_a read query should succeed");
        assert!(
            observed_rows.len() >= 2,
            "expected tx_a to observe at least two rows in the indexed range"
        );

        let mut tx_b = table
            .begin_transaction()
            .expect("tx_b begin_transaction failed");
        let mut row_0 = table
            .read(&mut tx_b, 0)
            .expect("tx_b read row 0 failed")
            .expect("row 0 missing");
        row_0.alt += 500;
        table
            .write(&mut tx_b, 0, row_0)
            .expect("tx_b write row 0 failed");
        table
            .commit_with_record(&mut tx_b)
            .expect("tx_b commit should succeed");

        let mut row_1 = table
            .read(&mut tx_a, 1)
            .expect("tx_a read row 1 failed")
            .expect("row 1 missing");
        row_1.alt += 250;
        table
            .write(&mut tx_a, 1, row_1)
            .expect("tx_a write row 1 failed");

        let err = table
            .commit_with_record(&mut tx_a)
            .expect_err("tx_a commit should fail SSI validation due to read-set conflict");
        assert_eq!(err, OccError::SerializationFailure);
    }
}
