use std::cmp::Ordering;
use std::sync::Arc;

use crate::rbo_planner::StapiRow;
use crate::stapi_parser::{Filter, Value};

pub(crate) type RowPredicate<T> = Arc<dyn Fn(&T) -> bool + Send + Sync>;

pub(crate) fn field_name(filter: &Filter) -> &str {
    match filter {
        Filter::Null { field } => field.as_str(),
        Filter::NotNull { field } => field.as_str(),
        Filter::Eq { field, .. } => field.as_str(),
        Filter::Ne { field, .. } => field.as_str(),
        Filter::Lt { field, .. } => field.as_str(),
        Filter::Lte { field, .. } => field.as_str(),
        Filter::Gt { field, .. } => field.as_str(),
        Filter::Gte { field, .. } => field.as_str(),
        Filter::In { field, .. } => field.as_str(),
        Filter::Match { field, .. } => field.as_str(),
        Filter::NotMatch { field, .. } => field.as_str(),
    }
}

pub(crate) fn compile_filter<T: StapiRow>(filter: &Filter) -> RowPredicate<T> {
    match filter {
        Filter::Null { field } => {
            let field = field.clone();
            Arc::new(move |row: &T| row.is_field_null(field.as_str()))
        }
        Filter::NotNull { field } => {
            let field = field.clone();
            Arc::new(move |row: &T| !row.is_field_null(field.as_str()))
        }
        Filter::Eq { field, value } => {
            let field = field.clone();
            let expected = value.clone();
            Arc::new(move |row: &T| {
                if row.is_field_null(field.as_str()) {
                    return false;
                }
                row.field_value(field.as_str())
                    .map(|actual| values_equal(&actual, &expected))
                    .unwrap_or(false)
            })
        }
        Filter::Ne { field, value } => {
            let field = field.clone();
            let expected = value.clone();
            Arc::new(move |row: &T| {
                if row.is_field_null(field.as_str()) {
                    return false;
                }
                row.field_value(field.as_str())
                    .map(|actual| !values_equal(&actual, &expected))
                    .unwrap_or(false)
            })
        }
        Filter::Lt { field, value } => {
            let field = field.clone();
            let expected = value.clone();
            Arc::new(move |row: &T| {
                if row.is_field_null(field.as_str()) {
                    return false;
                }
                row.field_value(field.as_str())
                    .map(|actual| {
                        matches!(compare_values(&actual, &expected), Some(Ordering::Less))
                    })
                    .unwrap_or(false)
            })
        }
        Filter::Lte { field, value } => {
            let field = field.clone();
            let expected = value.clone();
            Arc::new(move |row: &T| {
                if row.is_field_null(field.as_str()) {
                    return false;
                }
                row.field_value(field.as_str())
                    .map(|actual| {
                        matches!(
                            compare_values(&actual, &expected),
                            Some(Ordering::Less | Ordering::Equal)
                        )
                    })
                    .unwrap_or(false)
            })
        }
        Filter::Gt { field, value } => {
            let field = field.clone();
            let expected = value.clone();
            Arc::new(move |row: &T| {
                if row.is_field_null(field.as_str()) {
                    return false;
                }
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
                if row.is_field_null(field.as_str()) {
                    return false;
                }
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
                if row.is_field_null(field.as_str()) {
                    return false;
                }
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
                if row.is_field_null(field.as_str()) {
                    return false;
                }
                let Some(Value::Text(actual)) = row.field_value(field.as_str()) else {
                    return false;
                };
                glob_matches(pattern.as_str(), actual.as_str())
            })
        }
        Filter::NotMatch { field, pattern } => {
            let field = field.clone();
            let pattern = pattern.clone();
            Arc::new(move |row: &T| {
                if row.is_field_null(field.as_str()) {
                    return false;
                }
                let Some(Value::Text(actual)) = row.field_value(field.as_str()) else {
                    return false;
                };
                !glob_matches(pattern.as_str(), actual.as_str())
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
    use super::*;

    #[derive(Clone, Copy)]
    struct MaskedFlight {
        null_bitmask: u64,
        destination: [u8; 8],
        altitude: i64,
        typ: [u8; 8],
    }

    impl MaskedFlight {
        const NULLBIT_DESTINATION: u8 = 0;
        const NULLBIT_ALTITUDE: u8 = 1;
        const NULLBIT_TYP: u8 = 2;

        fn new(destination: [u8; 8], altitude: i64, typ: [u8; 8]) -> Self {
            Self {
                null_bitmask: 0,
                destination,
                altitude,
                typ,
            }
        }

        fn set_null_bit(&mut self, idx: u8, is_null: bool) {
            let bit = 1_u64 << idx;
            if is_null {
                self.null_bitmask |= bit;
            } else {
                self.null_bitmask &= !bit;
            }
        }
    }

    impl StapiRow for MaskedFlight {
        fn has_field(field: &str) -> bool {
            matches!(field, "destination" | "altitude" | "typ" | "type")
        }

        fn field_value(&self, field: &str) -> Option<Value> {
            match field {
                "destination" => Some(Value::Text(decode_ascii(&self.destination))),
                "altitude" => Some(Value::Int(self.altitude)),
                "typ" | "type" => Some(Value::Text(decode_ascii(&self.typ))),
                _ => None,
            }
        }

        fn is_field_null(&self, field: &str) -> bool {
            match field {
                "destination" => (self.null_bitmask & (1_u64 << Self::NULLBIT_DESTINATION)) != 0,
                "altitude" => (self.null_bitmask & (1_u64 << Self::NULLBIT_ALTITUDE)) != 0,
                "typ" | "type" => (self.null_bitmask & (1_u64 << Self::NULLBIT_TYP)) != 0,
                _ => false,
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

    #[test]
    fn null_bitmask_masks_garbage_payload_bytes_without_option_storage() {
        let mut row = MaskedFlight::new(fixed_ascii("JUNK"), 9_000, fixed_ascii("C17A"));
        row.set_null_bit(MaskedFlight::NULLBIT_DESTINATION, true);

        let is_null = compile_filter::<MaskedFlight>(&Filter::Null {
            field: "destination".to_string(),
        });
        let is_not_null = compile_filter::<MaskedFlight>(&Filter::NotNull {
            field: "destination".to_string(),
        });
        let eq_garbage = compile_filter::<MaskedFlight>(&Filter::Eq {
            field: "destination".to_string(),
            value: Value::Text("JUNK".to_string()),
        });
        let notmatch = compile_filter::<MaskedFlight>(&Filter::NotMatch {
            field: "destination".to_string(),
            pattern: "C17*".to_string(),
        });

        assert!(is_null(&row), "null bit must dominate physical bytes");
        assert!(!is_not_null(&row), "notnull must fail for masked columns");
        assert!(
            !eq_garbage(&row),
            "equality must not read garbage bytes when null bit is set"
        );
        assert!(
            !notmatch(&row),
            "negative text operators must also short-circuit on null"
        );
    }

    #[test]
    fn filters_short_circuit_all_non_null_operators_when_field_is_null() {
        let mut row = MaskedFlight::new(fixed_ascii("JUNK"), 9_000, fixed_ascii("C17A"));
        row.set_null_bit(MaskedFlight::NULLBIT_DESTINATION, true);
        row.set_null_bit(MaskedFlight::NULLBIT_ALTITUDE, true);
        row.set_null_bit(MaskedFlight::NULLBIT_TYP, true);

        let eq = compile_filter::<MaskedFlight>(&Filter::Eq {
            field: "destination".to_string(),
            value: Value::Text("JUNK".to_string()),
        });
        let ne = compile_filter::<MaskedFlight>(&Filter::Ne {
            field: "destination".to_string(),
            value: Value::Text("OTHER".to_string()),
        });
        let lt = compile_filter::<MaskedFlight>(&Filter::Lt {
            field: "altitude".to_string(),
            value: Value::Int(10_000),
        });
        let lte = compile_filter::<MaskedFlight>(&Filter::Lte {
            field: "altitude".to_string(),
            value: Value::Int(9_000),
        });
        let gt = compile_filter::<MaskedFlight>(&Filter::Gt {
            field: "altitude".to_string(),
            value: Value::Int(5_000),
        });
        let gte = compile_filter::<MaskedFlight>(&Filter::Gte {
            field: "altitude".to_string(),
            value: Value::Int(9_000),
        });
        let in_list = compile_filter::<MaskedFlight>(&Filter::In {
            field: "type".to_string(),
            values: vec![
                Value::Text("C17A".to_string()),
                Value::Text("B738".to_string()),
            ],
        });
        let matches = compile_filter::<MaskedFlight>(&Filter::Match {
            field: "destination".to_string(),
            pattern: "J*".to_string(),
        });
        let notmatch = compile_filter::<MaskedFlight>(&Filter::NotMatch {
            field: "destination".to_string(),
            pattern: "C17*".to_string(),
        });

        assert!(!eq(&row));
        assert!(!ne(&row));
        assert!(!lt(&row));
        assert!(!lte(&row));
        assert!(!gt(&row));
        assert!(!gte(&row));
        assert!(!in_list(&row));
        assert!(!matches(&row));
        assert!(!notmatch(&row));
    }

    #[test]
    fn filters_null_and_notnull_depend_only_on_bitmask() {
        let mut row = MaskedFlight::new(fixed_ascii("JUNK"), 9_000, fixed_ascii("C17A"));

        let is_null = compile_filter::<MaskedFlight>(&Filter::Null {
            field: "destination".to_string(),
        });
        let is_not_null = compile_filter::<MaskedFlight>(&Filter::NotNull {
            field: "destination".to_string(),
        });

        row.set_null_bit(MaskedFlight::NULLBIT_DESTINATION, true);
        assert!(is_null(&row));
        assert!(!is_not_null(&row));

        row.set_null_bit(MaskedFlight::NULLBIT_DESTINATION, false);
        assert!(!is_null(&row));
        assert!(is_not_null(&row));
    }

    #[test]
    fn filters_non_null_paths_remain_correct_for_in_range_and_negative_ops() {
        let row = MaskedFlight::new(fixed_ascii("KORD"), 12_000, fixed_ascii("B738"));

        let in_list = compile_filter::<MaskedFlight>(&Filter::In {
            field: "type".to_string(),
            values: vec![
                Value::Text("A320".to_string()),
                Value::Text("B738".to_string()),
            ],
        });
        let gte = compile_filter::<MaskedFlight>(&Filter::Gte {
            field: "altitude".to_string(),
            value: Value::Int(11_000),
        });
        let ne = compile_filter::<MaskedFlight>(&Filter::Ne {
            field: "typ".to_string(),
            value: Value::Text("C17A".to_string()),
        });
        let notmatch = compile_filter::<MaskedFlight>(&Filter::NotMatch {
            field: "type".to_string(),
            pattern: "C17*".to_string(),
        });

        assert!(in_list(&row), "in-list should match non-null typ");
        assert!(gte(&row), ">= should match non-null altitude");
        assert!(ne(&row), "!= should succeed on non-equal values");
        assert!(notmatch(&row), "notmatch should invert wildcard match");
    }

    #[test]
    fn filters_handle_negative_and_range_operators_correctly() {
        let row = MaskedFlight::new(fixed_ascii("KORD"), 12_000, fixed_ascii("B738"));

        let lte = compile_filter::<MaskedFlight>(&Filter::Lte {
            field: "altitude".to_string(),
            value: Value::Int(12_000),
        });
        let ne = compile_filter::<MaskedFlight>(&Filter::Ne {
            field: "typ".to_string(),
            value: Value::Text("C17A".to_string()),
        });
        let notmatch = compile_filter::<MaskedFlight>(&Filter::NotMatch {
            field: "type".to_string(),
            pattern: "C17*".to_string(),
        });

        assert!(lte(&row), "<= should include equal bound");
        assert!(ne(&row), "!= should succeed on non-equal values");
        assert!(notmatch(&row), "notmatch should invert wildcard match");
    }
}
