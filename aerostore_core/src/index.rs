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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexCompare {
    Eq(IndexValue),
    Gt(IndexValue),
    Gte(IndexValue),
    Lt(IndexValue),
    Lte(IndexValue),
    In(Vec<IndexValue>),
}

pub use crate::shm_index::SecondaryIndex;
