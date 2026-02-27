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
    Eq { field: String, value: Value },
    Lt { field: String, value: Value },
    Gt { field: String, value: Value },
    In { field: String, values: Vec<Value> },
    Match { field: String, pattern: String },
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
    if parts.len() < 3 {
        return false;
    }

    matches!(parts[0].as_str(), "=" | "==" | ">" | "<" | "in" | "IN" | "match")
}

fn parse_compare_clause(clause: &str) -> Result<Filter, ParseError> {
    let parts = parse_word_list(clause)?;
    if parts.is_empty() {
        return Err(ParseError::new("compare clause must not be empty"));
    }

    let op = parts[0].as_str();
    match op {
        "=" | "==" => {
            ensure_clause_arity(&parts, 3, "=")?;
            Ok(Filter::Eq {
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
        "<" => {
            ensure_clause_arity(&parts, 3, "<")?;
            Ok(Filter::Lt {
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
        "in" | "IN" => {
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
            op
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
    use super::{parse_stapi_query, Filter, ParseError, Value};

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
            "-compare {{= typ B738} {< alt 40000} {> alt 10000} {in typ {B738 A320}} {match flight UAL*}}",
        )
        .expect("operator matrix should parse");

        assert_eq!(parsed.filters.len(), 5);
        assert!(matches!(parsed.filters[0], Filter::Eq { .. }));
        assert!(matches!(parsed.filters[1], Filter::Lt { .. }));
        assert!(matches!(parsed.filters[2], Filter::Gt { .. }));
        assert!(matches!(parsed.filters[3], Filter::In { .. }));
        assert!(matches!(parsed.filters[4], Filter::Match { .. }));
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
            parse_stapi_query("-compare {{{> alt 10000}}}")
                .map_err(|err| err.tcl_error_message())
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
}
