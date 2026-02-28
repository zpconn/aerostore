use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::execution::{PrimaryKeyMapError, ShmPrimaryKeyMap};
use crate::index::{IndexCompare, IndexValue, SecondaryIndex};
use crate::occ::Error as OccError;
use crate::stapi_parser::{parse_stapi_query, Filter, ParseError, Query, Value};

pub(crate) type RowPredicate<T> = Arc<dyn Fn(&T) -> bool + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteKind {
    PrimaryKeyLookup,
    IndexExactMatch,
    IndexRangeScan,
    FullScan,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlannerError {
    Parse(String),
    UnknownField(String),
    InvalidIndexedValue(String),
    Occ(String),
    PrimaryKey(String),
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
            PlannerError::PrimaryKey(msg) => write!(f, "primary key map error: {}", msg),
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

impl From<PrimaryKeyMapError> for PlannerError {
    fn from(value: PrimaryKeyMapError) -> Self {
        PlannerError::PrimaryKey(value.to_string())
    }
}

pub trait StapiRow: Copy + Send + Sync + 'static {
    fn has_field(field: &str) -> bool;
    fn field_value(&self, field: &str) -> Option<Value>;
}

#[derive(Clone)]
pub struct SchemaCatalog {
    primary_key_field: String,
    primary_key_map: Option<Arc<ShmPrimaryKeyMap>>,
    indexes: HashMap<String, Arc<SecondaryIndex<usize>>>,
    cardinality_rank: HashMap<String, u32>,
}

impl SchemaCatalog {
    #[inline]
    pub fn new(primary_key_field: impl Into<String>) -> Self {
        let mut cardinality_rank = HashMap::new();
        cardinality_rank.insert("flight_id".to_string(), 0);
        cardinality_rank.insert("geohash".to_string(), 1);
        cardinality_rank.insert("dest".to_string(), 2);
        cardinality_rank.insert("altitude".to_string(), 3);
        cardinality_rank.insert("alt".to_string(), 3);

        Self {
            primary_key_field: primary_key_field.into(),
            primary_key_map: None,
            indexes: HashMap::new(),
            cardinality_rank,
        }
    }

    #[inline]
    pub fn with_primary_key_map(mut self, primary_key_map: Arc<ShmPrimaryKeyMap>) -> Self {
        self.primary_key_map = Some(primary_key_map);
        self
    }

    #[inline]
    pub fn insert_index(&mut self, field: impl Into<String>, index: Arc<SecondaryIndex<usize>>) {
        self.indexes.insert(field.into(), index);
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
    pub fn set_cardinality_rank(&mut self, field: impl Into<String>, rank: u32) {
        self.cardinality_rank.insert(field.into(), rank);
    }

    #[inline]
    pub fn primary_key_field(&self) -> &str {
        self.primary_key_field.as_str()
    }

    #[inline]
    pub fn primary_key_map(&self) -> Option<&Arc<ShmPrimaryKeyMap>> {
        self.primary_key_map.as_ref()
    }

    #[inline]
    pub(crate) fn get_index(&self, field: &str) -> Option<&Arc<SecondaryIndex<usize>>> {
        self.indexes.get(field)
    }

    #[inline]
    pub(crate) fn contains_index(&self, field: &str) -> bool {
        self.indexes.contains_key(field)
    }

    #[inline]
    pub(crate) fn rank_for(&self, field: &str) -> u32 {
        *self.cardinality_rank.get(field).unwrap_or(&u32::MAX)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AccessPath {
    PrimaryKeyEq {
        field: String,
        key: String,
    },
    Indexed {
        field: String,
        compare: IndexCompare,
    },
    FullScan,
}

pub(crate) struct CompiledFilter<T: StapiRow> {
    pub(crate) field: String,
    pub(crate) predicate: RowPredicate<T>,
}

pub struct CompiledPlan<T: StapiRow> {
    pub(crate) catalog: SchemaCatalog,
    pub(crate) route_kind: RouteKind,
    pub(crate) access_path: AccessPath,
    pub(crate) residual_filters: Vec<CompiledFilter<T>>,
    pub(crate) sort: Option<String>,
    pub(crate) limit: Option<usize>,
}

impl<T: StapiRow> CompiledPlan<T> {
    #[inline]
    pub fn route_kind(&self) -> RouteKind {
        self.route_kind
    }

    #[inline]
    pub fn access_path(&self) -> &AccessPath {
        &self.access_path
    }

    #[inline]
    pub fn driver_field(&self) -> Option<&str> {
        match &self.access_path {
            AccessPath::PrimaryKeyEq { field, .. } => Some(field.as_str()),
            AccessPath::Indexed { field, .. } => Some(field.as_str()),
            AccessPath::FullScan => None,
        }
    }

    #[inline]
    pub fn residual_filter_fields(&self) -> Vec<&str> {
        self.residual_filters
            .iter()
            .map(|filter| filter.field.as_str())
            .collect()
    }

    #[inline]
    pub fn sort_field(&self) -> Option<&str> {
        self.sort.as_deref()
    }

    #[inline]
    pub fn limit(&self) -> Option<usize> {
        self.limit
    }
}

pub struct RuleBasedOptimizer<T: StapiRow> {
    catalog: SchemaCatalog,
    _marker: PhantomData<T>,
}

impl<T: StapiRow> RuleBasedOptimizer<T> {
    #[inline]
    pub fn new(catalog: SchemaCatalog) -> Self {
        Self {
            catalog,
            _marker: PhantomData,
        }
    }

    pub fn compile_from_stapi(&self, stapi: &str) -> Result<CompiledPlan<T>, PlannerError> {
        let query = parse_stapi_query(stapi)?;
        self.compile(&query)
    }

    pub fn compile(&self, query: &Query) -> Result<CompiledPlan<T>, PlannerError> {
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

        let (route_kind, access_path, driver_filter_idx) = self.select_access_path(query)?;
        let mut residual_filters = Vec::with_capacity(query.filters.len());

        for (idx, filter) in query.filters.iter().enumerate() {
            if Some(idx) == driver_filter_idx {
                continue;
            }
            residual_filters.push(CompiledFilter {
                field: field_name(filter).to_string(),
                predicate: compile_filter::<T>(filter),
            });
        }

        Ok(CompiledPlan {
            catalog: self.catalog.clone(),
            route_kind,
            access_path,
            residual_filters,
            sort: query.sort.clone(),
            limit: query.limit,
        })
    }

    fn select_access_path(
        &self,
        query: &Query,
    ) -> Result<(RouteKind, AccessPath, Option<usize>), PlannerError> {
        #[derive(Clone)]
        struct Candidate {
            precedence: u8,
            rank: u32,
            filter_idx: usize,
            route_kind: RouteKind,
            access_path: AccessPath,
        }

        fn is_better(candidate: &Candidate, best: &Candidate) -> bool {
            (candidate.precedence, candidate.rank, candidate.filter_idx)
                < (best.precedence, best.rank, best.filter_idx)
        }

        let mut best: Option<Candidate> = None;
        for (idx, filter) in query.filters.iter().enumerate() {
            match filter {
                Filter::Eq { field, value } => {
                    // Rule 1: exact match on the primary key routes to the O(1) shared PK map.
                    if field.as_str() == self.catalog.primary_key_field()
                        && self.catalog.primary_key_map().is_some()
                    {
                        if let Some(key) = value_to_primary_key(value) {
                            let candidate = Candidate {
                                precedence: 1,
                                rank: 0,
                                filter_idx: idx,
                                route_kind: RouteKind::PrimaryKeyLookup,
                                access_path: AccessPath::PrimaryKeyEq {
                                    field: field.clone(),
                                    key,
                                },
                            };
                            if best
                                .as_ref()
                                .map(|b| is_better(&candidate, b))
                                .unwrap_or(true)
                            {
                                best = Some(candidate);
                            }
                        }
                    }

                    // Rule 2: exact match on an indexed column.
                    if self.catalog.contains_index(field.as_str()) {
                        let indexed_value = value_to_index(value).ok_or_else(|| {
                            PlannerError::InvalidIndexedValue(format!(
                                "field '{}' cannot use this value in an indexed equality route",
                                field
                            ))
                        })?;
                        let candidate = Candidate {
                            precedence: 2,
                            rank: self.catalog.rank_for(field.as_str()),
                            filter_idx: idx,
                            route_kind: RouteKind::IndexExactMatch,
                            access_path: AccessPath::Indexed {
                                field: field.clone(),
                                compare: IndexCompare::Eq(indexed_value),
                            },
                        };
                        if best
                            .as_ref()
                            .map(|b| is_better(&candidate, b))
                            .unwrap_or(true)
                        {
                            best = Some(candidate);
                        }
                    }
                }
                Filter::Gt { field, value } => {
                    if self.catalog.contains_index(field.as_str()) {
                        let indexed_value = value_to_index(value).ok_or_else(|| {
                            PlannerError::InvalidIndexedValue(format!(
                                "field '{}' cannot use this value in an indexed range route",
                                field
                            ))
                        })?;
                        let candidate = Candidate {
                            precedence: 3,
                            rank: self.catalog.rank_for(field.as_str()),
                            filter_idx: idx,
                            route_kind: RouteKind::IndexRangeScan,
                            access_path: AccessPath::Indexed {
                                field: field.clone(),
                                compare: IndexCompare::Gt(indexed_value),
                            },
                        };
                        if best
                            .as_ref()
                            .map(|b| is_better(&candidate, b))
                            .unwrap_or(true)
                        {
                            best = Some(candidate);
                        }
                    }
                }
                Filter::Lt { field, value } => {
                    if self.catalog.contains_index(field.as_str()) {
                        let indexed_value = value_to_index(value).ok_or_else(|| {
                            PlannerError::InvalidIndexedValue(format!(
                                "field '{}' cannot use this value in an indexed range route",
                                field
                            ))
                        })?;
                        let candidate = Candidate {
                            precedence: 3,
                            rank: self.catalog.rank_for(field.as_str()),
                            filter_idx: idx,
                            route_kind: RouteKind::IndexRangeScan,
                            access_path: AccessPath::Indexed {
                                field: field.clone(),
                                compare: IndexCompare::Lt(indexed_value),
                            },
                        };
                        if best
                            .as_ref()
                            .map(|b| is_better(&candidate, b))
                            .unwrap_or(true)
                        {
                            best = Some(candidate);
                        }
                    }
                }
                Filter::Gte { field, value } => {
                    if self.catalog.contains_index(field.as_str()) {
                        let indexed_value = value_to_index(value).ok_or_else(|| {
                            PlannerError::InvalidIndexedValue(format!(
                                "field '{}' cannot use this value in an indexed range route",
                                field
                            ))
                        })?;
                        let candidate = Candidate {
                            precedence: 3,
                            rank: self.catalog.rank_for(field.as_str()),
                            filter_idx: idx,
                            route_kind: RouteKind::IndexRangeScan,
                            access_path: AccessPath::Indexed {
                                field: field.clone(),
                                compare: IndexCompare::Gte(indexed_value),
                            },
                        };
                        if best
                            .as_ref()
                            .map(|b| is_better(&candidate, b))
                            .unwrap_or(true)
                        {
                            best = Some(candidate);
                        }
                    }
                }
                Filter::In { .. } | Filter::Match { .. } => {}
            }
        }

        if let Some(candidate) = best {
            Ok((
                candidate.route_kind,
                candidate.access_path,
                Some(candidate.filter_idx),
            ))
        } else {
            Ok((RouteKind::FullScan, AccessPath::FullScan, None))
        }
    }
}

fn field_name(filter: &Filter) -> &str {
    match filter {
        Filter::Eq { field, .. } => field.as_str(),
        Filter::Lt { field, .. } => field.as_str(),
        Filter::Gt { field, .. } => field.as_str(),
        Filter::Gte { field, .. } => field.as_str(),
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
        Filter::Gte { field, value } => {
            let field = field.clone();
            let expected = value.clone();
            Arc::new(move |row: &T| {
                row.field_value(field.as_str())
                    .map(|actual| {
                        matches!(
                            compare_values(&actual, &expected),
                            Some(Ordering::Greater | Ordering::Equal)
                        )
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

pub(crate) fn compare_optional(left: Option<Value>, right: Option<Value>) -> Ordering {
    match (left, right) {
        (Some(lhs), Some(rhs)) => compare_values(&lhs, &rhs).unwrap_or(Ordering::Equal),
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (None, None) => Ordering::Equal,
    }
}

fn values_equal(left: &Value, right: &Value) -> bool {
    matches!(compare_values(left, right), Some(Ordering::Equal))
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

fn value_to_primary_key(value: &Value) -> Option<String> {
    match value {
        Value::Text(v) => Some(v.clone()),
        Value::Int(v) => Some(v.to_string()),
        Value::Float(v) => {
            if v.is_finite() {
                Some(v.to_string())
            } else {
                None
            }
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
