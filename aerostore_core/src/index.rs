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

#[cfg(test)]
mod tests {
    use super::{IndexValue, IntoIndexValue};

    #[test]
    fn into_index_value_signed_maps_to_i64_variants() {
        assert_eq!((-1_i8).into_index_value(), IndexValue::I64(-1));
        assert_eq!((-2_i16).into_index_value(), IndexValue::I64(-2));
        assert_eq!((-3_i32).into_index_value(), IndexValue::I64(-3));
        assert_eq!((-4_i64).into_index_value(), IndexValue::I64(-4));
    }

    #[test]
    fn into_index_value_unsigned_maps_to_u64_variants() {
        assert_eq!((1_u8).into_index_value(), IndexValue::U64(1));
        assert_eq!((2_u16).into_index_value(), IndexValue::U64(2));
        assert_eq!((3_u32).into_index_value(), IndexValue::U64(3));
        assert_eq!((4_u64).into_index_value(), IndexValue::U64(4));
        assert_eq!((5_usize).into_index_value(), IndexValue::U64(5));
    }

    #[test]
    fn into_index_value_string_owned_and_borrowed_equivalent() {
        let borrowed = "UAL123".into_index_value();
        let owned = String::from("UAL123").into_index_value();
        assert_eq!(borrowed, owned);
    }

    #[test]
    fn index_value_ordering_is_stable_across_variants() {
        let mut values = vec![
            IndexValue::String("B".to_string()),
            IndexValue::U64(2),
            IndexValue::I64(1),
            IndexValue::String("A".to_string()),
            IndexValue::U64(1),
            IndexValue::I64(2),
        ];
        values.sort();
        assert_eq!(
            values,
            vec![
                IndexValue::I64(1),
                IndexValue::I64(2),
                IndexValue::U64(1),
                IndexValue::U64(2),
                IndexValue::String("A".to_string()),
                IndexValue::String("B".to_string()),
            ]
        );
    }
}
