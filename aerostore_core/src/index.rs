use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Arc;

use crossbeam_skiplist::{SkipMap, SkipSet};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IndexValue {
    I64(i64),
    U64(u64),
    String(String),
}

pub trait IntoIndexValue {
    fn into_index_value(self) -> IndexValue;
}

impl IntoIndexValue for i8 {
    fn into_index_value(self) -> IndexValue {
        IndexValue::I64(self as i64)
    }
}

impl IntoIndexValue for i16 {
    fn into_index_value(self) -> IndexValue {
        IndexValue::I64(self as i64)
    }
}

impl IntoIndexValue for i32 {
    fn into_index_value(self) -> IndexValue {
        IndexValue::I64(self as i64)
    }
}

impl IntoIndexValue for i64 {
    fn into_index_value(self) -> IndexValue {
        IndexValue::I64(self)
    }
}

impl IntoIndexValue for u8 {
    fn into_index_value(self) -> IndexValue {
        IndexValue::U64(self as u64)
    }
}

impl IntoIndexValue for u16 {
    fn into_index_value(self) -> IndexValue {
        IndexValue::U64(self as u64)
    }
}

impl IntoIndexValue for u32 {
    fn into_index_value(self) -> IndexValue {
        IndexValue::U64(self as u64)
    }
}

impl IntoIndexValue for u64 {
    fn into_index_value(self) -> IndexValue {
        IndexValue::U64(self)
    }
}

impl IntoIndexValue for usize {
    fn into_index_value(self) -> IndexValue {
        IndexValue::U64(self as u64)
    }
}

impl IntoIndexValue for String {
    fn into_index_value(self) -> IndexValue {
        IndexValue::String(self)
    }
}

impl IntoIndexValue for &str {
    fn into_index_value(self) -> IndexValue {
        IndexValue::String(self.to_string())
    }
}

#[derive(Clone, Debug)]
pub enum IndexCompare {
    Eq(IndexValue),
    Gt(IndexValue),
    Lt(IndexValue),
    In(Vec<IndexValue>),
}

pub struct SecondaryIndex<RowId>
where
    RowId: Ord + Clone + Send + Sync + 'static,
{
    field: &'static str,
    postings: SkipMap<IndexValue, Arc<SkipSet<RowId>>>,
}

impl<RowId> SecondaryIndex<RowId>
where
    RowId: Ord + Clone + Send + Sync + 'static,
{
    pub fn new(field: &'static str) -> Self {
        Self {
            field,
            postings: SkipMap::new(),
        }
    }

    #[inline]
    pub fn field(&self) -> &'static str {
        self.field
    }

    pub fn insert(&self, indexed_value: IndexValue, row_id: RowId) {
        let entry = self
            .postings
            .get_or_insert(indexed_value, Arc::new(SkipSet::new()));
        entry.value().insert(row_id);
    }

    pub fn remove(&self, indexed_value: &IndexValue, row_id: &RowId) {
        let Some(entry) = self.postings.get(indexed_value) else {
            return;
        };

        entry.value().remove(row_id);
    }

    pub fn lookup(&self, predicate: &IndexCompare) -> Vec<RowId> {
        match predicate {
            IndexCompare::Eq(v) => self.lookup_eq(v),
            IndexCompare::Gt(v) => {
                let mut out = BTreeSet::new();
                for entry in self.postings.range((Excluded(v.clone()), Unbounded)) {
                    self.extend_posting_set(&mut out, entry.value());
                }
                out.into_iter().collect()
            }
            IndexCompare::Lt(v) => {
                let mut out = BTreeSet::new();
                for entry in self.postings.range((Unbounded, Excluded(v.clone()))) {
                    self.extend_posting_set(&mut out, entry.value());
                }
                out.into_iter().collect()
            }
            IndexCompare::In(values) => {
                let mut out = BTreeSet::new();
                for value in values {
                    if let Some(entry) = self.postings.get(value) {
                        self.extend_posting_set(&mut out, entry.value());
                    }
                }
                out.into_iter().collect()
            }
        }
    }

    fn lookup_eq(&self, value: &IndexValue) -> Vec<RowId> {
        let Some(entry) = self.postings.get(value) else {
            return Vec::new();
        };

        entry
            .value()
            .iter()
            .map(|row| row.value().clone())
            .collect()
    }

    fn extend_posting_set(&self, out: &mut BTreeSet<RowId>, set: &Arc<SkipSet<RowId>>) {
        for row_id in set.iter() {
            out.insert(row_id.value().clone());
        }
    }
}
