use std::fmt;

use nom::branch::alt;
use nom::bytes::complete::take_while1;
use nom::character::complete::{char, multispace0};
use nom::combinator::{all_consuming, map};
use nom::multi::many0;
use nom::sequence::terminated;
use nom::IResult;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Text(String),
}

impl Value {
    #[inline]
    pub fn from_atom(atom: &str) -> Self {
        if let Ok(value) = atom.parse::<i64>() {
            return Value::Int(value);
        }

        if atom.contains('.') || atom.contains('e') || atom.contains('E') {
            if let Ok(value) = atom.parse::<f64>() {
                return Value::Float(value);
            }
        }

        Value::Text(atom.to_string())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Filter {
    Null { field: String },
    NotNull { field: String },
    Eq { field: String, value: Value },
    Ne { field: String, value: Value },
    Lt { field: String, value: Value },
    Lte { field: String, value: Value },
    Gt { field: String, value: Value },
    Gte { field: String, value: Value },
    In { field: String, values: Vec<Value> },
    Match { field: String, pattern: String },
    NotMatch { field: String, pattern: String },
}

impl Filter {
    #[inline]
    pub fn is_negative(&self) -> bool {
        matches!(self, Filter::Ne { .. } | Filter::NotMatch { .. })
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Query {
    pub filters: Vec<Filter>,
    pub sort: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParseError {
    message: String,
}

impl ParseError {
    #[inline]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    #[inline]
    pub fn message(&self) -> &str {
        self.message.as_str()
    }

    #[inline]
    pub fn tcl_error_message(&self) -> String {
        format!("TCL_ERROR: {}", self.message)
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message.as_str())
    }
}

impl std::error::Error for ParseError {}

pub fn parse_stapi_query(input: &str) -> Result<Query, ParseError> {
    let tokens = parse_word_list(input)?;
    parse_query_tokens(tokens)
}

fn parse_query_tokens(tokens: Vec<String>) -> Result<Query, ParseError> {
    let mut query = Query::default();
    let mut idx = 0_usize;

    while idx < tokens.len() {
        match tokens[idx].as_str() {
            "-compare" => {
                if idx + 1 >= tokens.len() {
                    return Err(ParseError::new("missing value for -compare"));
                }
                let mut parsed = parse_compare_clauses(tokens[idx + 1].as_str())?;
                query.filters.append(&mut parsed);
                idx += 2;
            }
            "-sort" => {
                if idx + 1 >= tokens.len() {
                    return Err(ParseError::new("missing value for -sort"));
                }
                query.sort = Some(tokens[idx + 1].clone());
                idx += 2;
            }
            "-limit" => {
                if idx + 1 >= tokens.len() {
                    return Err(ParseError::new("missing value for -limit"));
                }
                let parsed = tokens[idx + 1]
                    .parse::<usize>()
                    .map_err(|_| ParseError::new("limit must be a non-negative integer"))?;
                query.limit = Some(parsed);
                idx += 2;
            }
            unknown => {
                return Err(ParseError::new(format!(
                    "unknown STAPI option '{}'",
                    unknown
                )))
            }
        }
    }

    Ok(query)
}

fn parse_compare_clauses(raw_compare: &str) -> Result<Vec<Filter>, ParseError> {
    let clauses = parse_word_list(raw_compare)?;
    if clauses.is_empty() {
        return Err(ParseError::new("compare list cannot be empty"));
    }

    if looks_like_single_clause(&clauses) {
        return Ok(vec![parse_compare_clause(raw_compare)?]);
    }

    let mut parsed = Vec::with_capacity(clauses.len());
    for clause in clauses {
        parsed.push(parse_compare_clause(clause.as_str())?);
    }
    Ok(parsed)
}

fn looks_like_single_clause(parts: &[String]) -> bool {
    if parts.len() == 2 {
        return matches!(parts[0].to_ascii_lowercase().as_str(), "null" | "notnull");
    }
    if parts.len() < 3 {
        return false;
    }
    matches!(
        parts[0].to_ascii_lowercase().as_str(),
        "=" | "==" | "!=" | "<>" | ">" | ">=" | "<" | "<=" | "in" | "match" | "notmatch"
    )
}

fn parse_compare_clause(clause: &str) -> Result<Filter, ParseError> {
    let parts = parse_word_list(clause)?;
    if parts.is_empty() {
        return Err(ParseError::new("compare clause must not be empty"));
    }

    let op = parts[0].to_ascii_lowercase();
    match op.as_str() {
        "null" => {
            ensure_clause_arity(&parts, 2, "null")?;
            Ok(Filter::Null {
                field: parts[1].clone(),
            })
        }
        "notnull" => {
            ensure_clause_arity(&parts, 2, "notnull")?;
            Ok(Filter::NotNull {
                field: parts[1].clone(),
            })
        }
        "=" | "==" => {
            ensure_clause_arity(&parts, 3, "=")?;
            Ok(Filter::Eq {
                field: parts[1].clone(),
                value: Value::from_atom(parts[2].as_str()),
            })
        }
        "!=" | "<>" => {
            ensure_clause_arity(&parts, 3, "!=")?;
            Ok(Filter::Ne {
                field: parts[1].clone(),
                value: Value::from_atom(parts[2].as_str()),
            })
        }
        ">" => {
            ensure_clause_arity(&parts, 3, ">")?;
            Ok(Filter::Gt {
                field: parts[1].clone(),
                value: Value::from_atom(parts[2].as_str()),
            })
        }
        ">=" => {
            ensure_clause_arity(&parts, 3, ">=")?;
            Ok(Filter::Gte {
                field: parts[1].clone(),
                value: Value::from_atom(parts[2].as_str()),
            })
        }
        "<" => {
            ensure_clause_arity(&parts, 3, "<")?;
            Ok(Filter::Lt {
                field: parts[1].clone(),
                value: Value::from_atom(parts[2].as_str()),
            })
        }
        "<=" => {
            ensure_clause_arity(&parts, 3, "<=")?;
            Ok(Filter::Lte {
                field: parts[1].clone(),
                value: Value::from_atom(parts[2].as_str()),
            })
        }
        "match" => {
            ensure_clause_arity(&parts, 3, "match")?;
            Ok(Filter::Match {
                field: parts[1].clone(),
                pattern: parts[2].clone(),
            })
        }
        "notmatch" => {
            ensure_clause_arity(&parts, 3, "notmatch")?;
            Ok(Filter::NotMatch {
                field: parts[1].clone(),
                pattern: parts[2].clone(),
            })
        }
        "in" => {
            ensure_clause_arity(&parts, 3, "in")?;
            let raw_values = parse_word_list(parts[2].as_str())?;
            if raw_values.is_empty() {
                return Err(ParseError::new(
                    "operator 'in' expects at least one value in the third argument",
                ));
            }
            let values = raw_values
                .iter()
                .map(|raw| Value::from_atom(raw.as_str()))
                .collect();

            Ok(Filter::In {
                field: parts[1].clone(),
                values,
            })
        }
        _ => Err(ParseError::new(format!(
            "unsupported compare operator '{}'",
            parts[0].as_str()
        ))),
    }
}

fn ensure_clause_arity(parts: &[String], expected: usize, op: &str) -> Result<(), ParseError> {
    if parts.len() != expected {
        return Err(ParseError::new(format!(
            "operator '{}' expects exactly {} items, got {}",
            op,
            expected,
            parts.len()
        )));
    }
    Ok(())
}

pub fn parse_word_list(input: &str) -> Result<Vec<String>, ParseError> {
    all_consuming(word_list_parser)(input)
        .map(|(_, words)| words)
        .map_err(|_| ParseError::new("invalid Tcl-style list syntax"))
}

fn word_list_parser(input: &str) -> IResult<&str, Vec<String>> {
    let (input, _) = multispace0(input)?;
    let (input, words) = many0(terminated(word_atom_parser, multispace0))(input)?;
    Ok((input, words))
}

fn word_atom_parser(input: &str) -> IResult<&str, String> {
    alt((braced_word_parser, bare_word_parser))(input)
}

fn bare_word_parser(input: &str) -> IResult<&str, String> {
    map(
        take_while1(|ch: char| !ch.is_whitespace() && ch != '{' && ch != '}'),
        |raw: &str| raw.to_string(),
    )(input)
}

fn braced_word_parser(input: &str) -> IResult<&str, String> {
    let (input, _) = char('{')(input)?;
    take_balanced_braced_word(input)
}

fn take_balanced_braced_word(input: &str) -> IResult<&str, String> {
    let mut depth = 1_usize;

    for (idx, ch) in input.char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    let content = input[..idx].to_string();
                    let rest = &input[idx + 1..];
                    return Ok((rest, content));
                }
            }
            _ => {}
        }
    }

    Err(nom::Err::Error(nom::error::Error::new(
        input,
        nom::error::ErrorKind::Eof,
    )))
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{parse_stapi_query, Filter, ParseError, Value};

    fn field_strategy() -> impl Strategy<Value = String> {
        "[a-z][a-z0-9_]{0,15}".prop_map(|s| s.to_string())
    }

    fn text_atom_strategy() -> impl Strategy<Value = String> {
        "[A-Za-z][A-Za-z0-9_]{0,15}".prop_map(|s| s.to_string())
    }

    #[test]
    fn parses_complex_nested_compare_list() {
        let parsed = parse_stapi_query(
            "-compare {{match flight UAL*} {> alt 10000} {in typ {B738 A320}}} -sort alt -limit 50",
        )
        .expect("complex STAPI query should parse");

        assert_eq!(parsed.sort.as_deref(), Some("alt"));
        assert_eq!(parsed.limit, Some(50));
        assert_eq!(parsed.filters.len(), 3);
        assert_eq!(
            parsed.filters[0],
            Filter::Match {
                field: "flight".to_string(),
                pattern: "UAL*".to_string(),
            }
        );
        assert_eq!(
            parsed.filters[1],
            Filter::Gt {
                field: "alt".to_string(),
                value: Value::Int(10_000),
            }
        );
        assert_eq!(
            parsed.filters[2],
            Filter::In {
                field: "typ".to_string(),
                values: vec![
                    Value::Text("B738".to_string()),
                    Value::Text("A320".to_string())
                ],
            }
        );
    }

    #[test]
    fn parses_each_supported_operator() {
        let parsed = parse_stapi_query(
            "-compare {{null destination} {notnull geohash} {= typ B738} {!= typ C17} {< alt 40000} {<= alt 40000} {> alt 10000} {>= alt 10000} {in typ {B738 A320}} {match flight UAL*} {notmatch typ C17*}}",
        )
        .expect("operator matrix should parse");

        assert_eq!(parsed.filters.len(), 11);
        assert!(matches!(parsed.filters[0], Filter::Null { .. }));
        assert!(matches!(parsed.filters[1], Filter::NotNull { .. }));
        assert!(matches!(parsed.filters[2], Filter::Eq { .. }));
        assert!(matches!(parsed.filters[3], Filter::Ne { .. }));
        assert!(matches!(parsed.filters[4], Filter::Lt { .. }));
        assert!(matches!(parsed.filters[5], Filter::Lte { .. }));
        assert!(matches!(parsed.filters[6], Filter::Gt { .. }));
        assert!(matches!(parsed.filters[7], Filter::Gte { .. }));
        assert!(matches!(parsed.filters[8], Filter::In { .. }));
        assert!(matches!(parsed.filters[9], Filter::Match { .. }));
        assert!(matches!(parsed.filters[10], Filter::NotMatch { .. }));
    }

    #[test]
    fn parses_operator_aliases_and_case_variants() {
        let parsed = parse_stapi_query(
            "-compare {{NuLl destination} {NoTnUlL geohash} {== typ B738} {<> typ C17} {NoTmAtCh type C17*}}",
        )
        .expect("operator aliases and mixed-case variants should parse");

        assert_eq!(parsed.filters.len(), 5);
        assert!(matches!(
            parsed.filters[0],
            Filter::Null { ref field } if field == "destination"
        ));
        assert!(matches!(
            parsed.filters[1],
            Filter::NotNull { ref field } if field == "geohash"
        ));
        assert!(matches!(
            parsed.filters[2],
            Filter::Eq { ref field, value: Value::Text(ref value) } if field == "typ" && value == "B738"
        ));
        assert!(matches!(
            parsed.filters[3],
            Filter::Ne { ref field, value: Value::Text(ref value) } if field == "typ" && value == "C17"
        ));
        assert!(parsed.filters[3].is_negative());
        assert!(matches!(
            parsed.filters[4],
            Filter::NotMatch { ref field, ref pattern } if field == "type" && pattern == "C17*"
        ));
        assert!(parsed.filters[4].is_negative());
    }

    #[test]
    fn parses_single_clause_unary_compare_form() {
        let parsed_null = parse_stapi_query("-compare {null destination}")
            .expect("single-clause unary null query should parse");
        assert_eq!(parsed_null.filters.len(), 1);
        assert!(matches!(
            parsed_null.filters[0],
            Filter::Null { ref field } if field == "destination"
        ));

        let parsed_notnull = parse_stapi_query("-compare {notnull geohash}")
            .expect("single-clause unary notnull query should parse");
        assert_eq!(parsed_notnull.filters.len(), 1);
        assert!(matches!(
            parsed_notnull.filters[0],
            Filter::NotNull { ref field } if field == "geohash"
        ));
    }

    #[test]
    fn parses_nested_negative_and_null_compare_clauses() {
        let parsed = parse_stapi_query(
            "-compare {{null destination} {<= altitude 10000} {notmatch type C17*} {!= ident UAL123} {notnull geohash}}",
        )
        .expect("nested negative/null clauses should parse");

        assert_eq!(parsed.filters.len(), 5);
        assert!(matches!(
            parsed.filters[0],
            Filter::Null { ref field } if field == "destination"
        ));
        assert!(matches!(
            parsed.filters[1],
            Filter::Lte { ref field, value: Value::Int(10_000) } if field == "altitude"
        ));
        assert!(matches!(
            parsed.filters[2],
            Filter::NotMatch { ref field, ref pattern } if field == "type" && pattern == "C17*"
        ));
        assert!(parsed.filters[2].is_negative());
        assert!(matches!(
            parsed.filters[3],
            Filter::Ne { ref field, value: Value::Text(ref value) } if field == "ident" && value == "UAL123"
        ));
        assert!(parsed.filters[3].is_negative());
        assert!(matches!(
            parsed.filters[4],
            Filter::NotNull { ref field } if field == "geohash"
        ));
    }

    #[test]
    fn rejects_invalid_null_operator_arity() {
        let err = parse_stapi_query("-compare {{null destination now}}")
            .expect_err("null operator with invalid arity must be rejected");
        assert!(err.tcl_error_message().starts_with("TCL_ERROR:"));
        assert!(err.message().contains("expects exactly 2"));
    }

    #[test]
    fn rejects_invalid_arity_for_notnull_notmatch_and_neq() {
        let err_notnull = parse_stapi_query("-compare {{notnull geohash now}}")
            .expect_err("notnull operator with invalid arity must be rejected");
        assert!(err_notnull.tcl_error_message().starts_with("TCL_ERROR:"));
        assert!(err_notnull
            .message()
            .contains("operator 'notnull' expects exactly 2"));

        let err_notmatch = parse_stapi_query("-compare {{notmatch type}}")
            .expect_err("notmatch operator with missing pattern must be rejected");
        assert!(err_notmatch.tcl_error_message().starts_with("TCL_ERROR:"));
        assert!(err_notmatch
            .message()
            .contains("operator 'notmatch' expects exactly 3"));

        let err_neq = parse_stapi_query("-compare {{!= ident}}")
            .expect_err("!= operator with missing rhs must be rejected");
        assert!(err_neq.tcl_error_message().starts_with("TCL_ERROR:"));
        assert!(err_neq
            .message()
            .contains("operator '!=' expects exactly 3"));
    }

    #[test]
    fn rejects_malformed_syntax_with_explicit_tcl_error() {
        let err = parse_stapi_query("-compare {{> alt 10000} {in typ {B738 A320}} -sort alt")
            .expect_err("unbalanced brace must be rejected");
        assert!(err.tcl_error_message().starts_with("TCL_ERROR:"));
        assert_eq!(err.message(), "invalid Tcl-style list syntax");
    }

    #[test]
    fn rejects_unknown_option_with_tcl_error_message() {
        let err = parse_stapi_query("-compare {{> alt 10000}} -bogus value")
            .expect_err("unknown option must be rejected");
        assert!(err.tcl_error_message().contains("TCL_ERROR:"));
        assert!(err.message().contains("unknown STAPI option"));
    }

    #[test]
    fn malformed_input_never_panics_across_boundary() {
        let result = std::panic::catch_unwind(|| {
            parse_stapi_query("-compare {{{> alt 10000}}}").map_err(|err| err.tcl_error_message())
        });

        assert!(result.is_ok(), "parser must not panic");
        let inner = result.expect("parser should not panic");
        assert!(inner.is_err(), "malformed syntax must return an error");
        let err = inner.expect_err("malformed syntax should fail");
        assert!(err.starts_with("TCL_ERROR:"));
    }

    #[test]
    fn parse_error_display_is_human_readable() {
        let err = ParseError::new("bad query");
        assert_eq!(err.to_string(), "bad query");
    }

    proptest! {
        #[test]
        fn parser_never_panics_for_arbitrary_input(input in ".{0,256}") {
            let result = std::panic::catch_unwind(|| parse_stapi_query(input.as_str()));
            prop_assert!(result.is_ok());
        }

        #[test]
        fn value_from_atom_parses_all_i64_values(v in any::<i64>()) {
            let atom = v.to_string();
            prop_assert_eq!(Value::from_atom(atom.as_str()), Value::Int(v));
        }

        #[test]
        fn parses_eq_clause_with_generated_identifiers(
            field in field_strategy(),
            value in text_atom_strategy(),
        ) {
            let query = format!("-compare {{= {} {}}}", field, value);
            let parsed = parse_stapi_query(query.as_str()).expect("generated equality query should parse");
            prop_assert_eq!(parsed.filters.len(), 1);
            prop_assert_eq!(
                &parsed.filters[0],
                &Filter::Eq {
                    field,
                    value: Value::Text(value),
                }
            );
        }

        #[test]
        fn parses_in_clause_with_generated_atoms(
            field in field_strategy(),
            values in prop::collection::vec(text_atom_strategy(), 1..8),
        ) {
            let values_expr = values.join(" ");
            let query = format!("-compare {{in {} {{{}}}}}", field, values_expr);
            let parsed = parse_stapi_query(query.as_str()).expect("generated in query should parse");
            prop_assert_eq!(parsed.filters.len(), 1);

            match &parsed.filters[0] {
                Filter::In { field: parsed_field, values: parsed_values } => {
                    prop_assert_eq!(parsed_field, &field);
                    let expected: Vec<Value> = values.into_iter().map(Value::Text).collect();
                    prop_assert_eq!(parsed_values, &expected);
                }
                other => prop_assert!(false, "expected Filter::In, got {:?}", other),
            }
        }
    }
}
