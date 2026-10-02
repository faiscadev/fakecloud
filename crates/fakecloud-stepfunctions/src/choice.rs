use std::cmp::Ordering;

use serde_json::Value;

use crate::io_processing::{resolve_with_context, StatesError};

/// Evaluate a Choice state's rules against the input and return the Next state name.
/// Returns `Ok(None)` if no rule matches and there's no Default.
///
/// A rule whose `Variable` (or `*Path` comparand) matches nothing fails the
/// state with `States.Runtime`, as on AWS; only an `IsPresent` test may probe
/// a missing field.
pub fn evaluate_choice(
    state_def: &Value,
    input: &Value,
    context: Option<&Value>,
) -> Result<Option<String>, StatesError> {
    if let Some(choices) = state_def["Choices"].as_array() {
        for choice in choices {
            if evaluate_rule(choice, input, context)? {
                return Ok(choice["Next"].as_str().map(|s| s.to_string()));
            }
        }
    }

    // Fall through to Default
    Ok(state_def["Default"].as_str().map(|s| s.to_string()))
}

fn invalid_variable(path: &str) -> StatesError {
    (
        "States.Runtime".to_string(),
        format!(
            "Invalid path '{path}': The choice state's condition path references an invalid value."
        ),
    )
}

/// Resolve a Choice path (`$...` against the input, `$$...` against the
/// context object), failing on a miss.
fn lookup(input: &Value, context: Option<&Value>, path: &str) -> Result<Value, StatesError> {
    resolve_with_context(input, context, path).map_err(|_| invalid_variable(path))
}

/// Evaluate a single choice rule (may be compound via And/Or/Not).
fn evaluate_rule(
    rule: &Value,
    input: &Value,
    context: Option<&Value>,
) -> Result<bool, StatesError> {
    if let Some(and_rules) = rule["And"].as_array() {
        for r in and_rules {
            if !evaluate_rule(r, input, context)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    if let Some(or_rules) = rule["Or"].as_array() {
        for r in or_rules {
            if evaluate_rule(r, input, context)? {
                return Ok(true);
            }
        }
        return Ok(false);
    }
    if rule.get("Not").is_some() {
        return Ok(!evaluate_rule(&rule["Not"], input, context)?);
    }

    let variable = match rule["Variable"].as_str() {
        Some(v) => v,
        None => return Ok(false),
    };

    // IsPresent is the one test defined on a missing field.
    if let Some(expected) = rule.get("IsPresent") {
        let is_present = resolve_with_context(input, context, variable).is_ok();
        return Ok(expected.as_bool().unwrap_or(false) == is_present);
    }

    let value = lookup(input, context, variable)?;

    if let Some(result) = evaluate_type_test(rule, &value) {
        return Ok(result);
    }

    for (op, kind) in COMPARATORS {
        if let Some(operand) = rule.get(*op) {
            return Ok(compare(*kind, op, operand, &value));
        }
        let path_op = format!("{op}Path");
        if let Some(path) = rule.get(path_op.as_str()) {
            let Some(path) = path.as_str() else {
                return Ok(false);
            };
            let other = lookup(input, context, path)?;
            return Ok(compare(*kind, op, &other, &value));
        }
    }

    Ok(false)
}

fn evaluate_type_test(rule: &Value, value: &Value) -> Option<bool> {
    let test = |key: &str, actual: bool| {
        rule.get(key)
            .map(|expected| expected.as_bool().unwrap_or(false) == actual)
    };
    test("IsNull", value.is_null())
        .or_else(|| test("IsNumeric", value.is_number()))
        .or_else(|| test("IsString", value.is_string()))
        .or_else(|| test("IsBoolean", value.is_boolean()))
        .or_else(|| {
            test(
                "IsTimestamp",
                value.as_str().and_then(parse_timestamp).is_some(),
            )
        })
}

#[derive(Clone, Copy)]
enum Kind {
    String,
    Numeric,
    Boolean,
    Timestamp,
    StringMatches,
}

/// Every ASL data-test comparator; each also has a `<name>Path` form whose
/// comparand is read from the input.
const COMPARATORS: &[(&str, Kind)] = &[
    ("StringEquals", Kind::String),
    ("StringLessThan", Kind::String),
    ("StringGreaterThan", Kind::String),
    ("StringLessThanEquals", Kind::String),
    ("StringGreaterThanEquals", Kind::String),
    ("StringMatches", Kind::StringMatches),
    ("NumericEquals", Kind::Numeric),
    ("NumericLessThan", Kind::Numeric),
    ("NumericGreaterThan", Kind::Numeric),
    ("NumericLessThanEquals", Kind::Numeric),
    ("NumericGreaterThanEquals", Kind::Numeric),
    ("BooleanEquals", Kind::Boolean),
    ("TimestampEquals", Kind::Timestamp),
    ("TimestampLessThan", Kind::Timestamp),
    ("TimestampGreaterThan", Kind::Timestamp),
    ("TimestampLessThanEquals", Kind::Timestamp),
    ("TimestampGreaterThanEquals", Kind::Timestamp),
];

/// Apply comparator `op` with comparand `operand` to `value`. A type mismatch
/// (e.g. a NumericEquals on a string) is simply false.
fn compare(kind: Kind, op: &str, operand: &Value, value: &Value) -> bool {
    let ordering = match kind {
        Kind::StringMatches => {
            return match (value.as_str(), operand.as_str()) {
                (Some(v), Some(p)) => string_matches(v, p),
                _ => false,
            };
        }
        Kind::Boolean => {
            return matches!((value.as_bool(), operand.as_bool()), (Some(a), Some(b)) if a == b);
        }
        Kind::String => match (value.as_str(), operand.as_str()) {
            (Some(v), Some(o)) => Some(v.cmp(o)),
            _ => None,
        },
        Kind::Numeric => match (value.as_f64(), operand.as_f64()) {
            (Some(v), Some(o)) => v.partial_cmp(&o),
            _ => None,
        },
        Kind::Timestamp => match (
            value.as_str().and_then(parse_timestamp),
            operand.as_str().and_then(parse_timestamp),
        ) {
            (Some(v), Some(o)) => Some(v.cmp(&o)),
            _ => None,
        },
    };
    let Some(ord) = ordering else {
        return false;
    };
    if op.ends_with("GreaterThanEquals") {
        ord != Ordering::Less
    } else if op.ends_with("LessThanEquals") {
        ord != Ordering::Greater
    } else if op.ends_with("GreaterThan") {
        ord == Ordering::Greater
    } else if op.ends_with("LessThan") {
        ord == Ordering::Less
    } else {
        ord == Ordering::Equal
    }
}

fn parse_timestamp(s: &str) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    chrono::DateTime::parse_from_rfc3339(s).ok()
}

/// Glob-style pattern matching for StringMatches.
/// Supports `*` (matches any sequence), `\*` (literal asterisk) and `\\`
/// (literal backslash).
fn string_matches(value: &str, pattern: &str) -> bool {
    let compiled = compile_glob_pattern(pattern);
    glob_dp_match(&value.chars().collect::<Vec<_>>(), &compiled)
}

/// Compile a glob pattern into a token vector where `GlobToken::Wildcard`
/// represents `*` and `GlobToken::Char(c)` represents a literal character
/// (including escaped `\*`).
fn compile_glob_pattern(pattern: &str) -> Vec<GlobToken> {
    let mut out = Vec::new();
    let chars: Vec<char> = pattern.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '\\' && i + 1 < chars.len() && matches!(chars[i + 1], '*' | '\\') {
            out.push(GlobToken::Char(chars[i + 1]));
            i += 2;
        } else if chars[i] == '*' {
            out.push(GlobToken::Wildcard);
            i += 1;
        } else {
            out.push(GlobToken::Char(chars[i]));
            i += 1;
        }
    }
    out
}

/// Match a compiled glob pattern against a value using dynamic programming.
fn glob_dp_match(value: &[char], pattern: &[GlobToken]) -> bool {
    let m = value.len();
    let n = pattern.len();
    let mut dp = vec![vec![false; n + 1]; m + 1];
    dp[0][0] = true;

    for j in 1..=n {
        if matches!(pattern[j - 1], GlobToken::Wildcard) {
            dp[0][j] = dp[0][j - 1];
        }
    }

    for i in 1..=m {
        for j in 1..=n {
            match pattern[j - 1] {
                GlobToken::Wildcard => {
                    dp[i][j] = dp[i][j - 1] || dp[i - 1][j];
                }
                GlobToken::Char(c) if c == value[i - 1] => {
                    dp[i][j] = dp[i - 1][j - 1];
                }
                GlobToken::Char(_) => {}
            }
        }
    }

    dp[m][n]
}

#[derive(Clone, Copy)]
enum GlobToken {
    Char(char),
    Wildcard,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_string_equals() {
        let rule = json!({
            "Variable": "$.status",
            "StringEquals": "active",
            "Next": "Active"
        });
        let input = json!({"status": "active"});
        assert!(evaluate_rule(&rule, &input, None).unwrap());

        let input = json!({"status": "inactive"});
        assert!(!evaluate_rule(&rule, &input, None).unwrap());
    }

    #[test]
    fn test_numeric_greater_than() {
        let rule = json!({
            "Variable": "$.count",
            "NumericGreaterThan": 10,
            "Next": "High"
        });
        let input = json!({"count": 15});
        assert!(evaluate_rule(&rule, &input, None).unwrap());

        let input = json!({"count": 5});
        assert!(!evaluate_rule(&rule, &input, None).unwrap());
    }

    #[test]
    fn test_boolean_equals() {
        let rule = json!({
            "Variable": "$.enabled",
            "BooleanEquals": true,
            "Next": "Enabled"
        });
        let input = json!({"enabled": true});
        assert!(evaluate_rule(&rule, &input, None).unwrap());

        let input = json!({"enabled": false});
        assert!(!evaluate_rule(&rule, &input, None).unwrap());
    }

    #[test]
    fn test_and_operator() {
        let rule = json!({
            "And": [
                {"Variable": "$.a", "NumericGreaterThan": 0},
                {"Variable": "$.b", "NumericLessThan": 100}
            ],
            "Next": "Both"
        });
        let input = json!({"a": 5, "b": 50});
        assert!(evaluate_rule(&rule, &input, None).unwrap());

        let input = json!({"a": -1, "b": 50});
        assert!(!evaluate_rule(&rule, &input, None).unwrap());
    }

    #[test]
    fn test_or_operator() {
        let rule = json!({
            "Or": [
                {"Variable": "$.status", "StringEquals": "active"},
                {"Variable": "$.status", "StringEquals": "pending"}
            ],
            "Next": "Valid"
        });
        let input = json!({"status": "active"});
        assert!(evaluate_rule(&rule, &input, None).unwrap());

        let input = json!({"status": "closed"});
        assert!(!evaluate_rule(&rule, &input, None).unwrap());
    }

    #[test]
    fn test_not_operator() {
        let rule = json!({
            "Not": {
                "Variable": "$.status",
                "StringEquals": "closed"
            },
            "Next": "Open"
        });
        let input = json!({"status": "active"});
        assert!(evaluate_rule(&rule, &input, None).unwrap());

        let input = json!({"status": "closed"});
        assert!(!evaluate_rule(&rule, &input, None).unwrap());
    }

    #[test]
    fn test_is_present() {
        let rule = json!({
            "Variable": "$.optional",
            "IsPresent": true,
            "Next": "HasField"
        });
        let input = json!({"optional": "value"});
        assert!(evaluate_rule(&rule, &input, None).unwrap());

        let input = json!({"other": "value"});
        assert!(!evaluate_rule(&rule, &input, None).unwrap());
    }

    #[test]
    fn test_is_present_with_array_index() {
        let rule = json!({
            "Variable": "$.items[0]",
            "IsPresent": true,
            "Next": "HasItem"
        });
        let input = json!({"items": [10, 20, 30]});
        assert!(evaluate_rule(&rule, &input, None).unwrap());

        let input = json!({"items": []});
        assert!(!evaluate_rule(&rule, &input, None).unwrap());
    }

    #[test]
    fn test_is_present_with_null_value() {
        // A field that is explicitly set to null should be considered "present"
        let rule = json!({
            "Variable": "$.optional",
            "IsPresent": true,
            "Next": "HasField"
        });
        let input = json!({"optional": null});
        assert!(evaluate_rule(&rule, &input, None).unwrap());
    }

    #[test]
    fn test_is_null() {
        let rule = json!({
            "Variable": "$.field",
            "IsNull": true,
            "Next": "Null"
        });
        let input = json!({"field": null});
        assert!(evaluate_rule(&rule, &input, None).unwrap());

        let input = json!({"field": "value"});
        assert!(!evaluate_rule(&rule, &input, None).unwrap());
    }

    #[test]
    fn test_is_numeric() {
        let rule = json!({
            "Variable": "$.value",
            "IsNumeric": true,
            "Next": "Number"
        });
        let input = json!({"value": 42});
        assert!(evaluate_rule(&rule, &input, None).unwrap());

        let input = json!({"value": "not a number"});
        assert!(!evaluate_rule(&rule, &input, None).unwrap());
    }

    #[test]
    fn test_string_matches() {
        assert!(string_matches("hello world", "hello*"));
        assert!(string_matches("hello world", "*world"));
        assert!(string_matches("hello world", "hello*world"));
        assert!(string_matches("hello world", "*"));
        assert!(!string_matches("hello world", "goodbye*"));
        assert!(string_matches("log-2024-01-15.txt", "log-*.txt"));
    }

    #[test]
    fn test_evaluate_choice_with_default() {
        let state_def = json!({
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.status",
                    "StringEquals": "active",
                    "Next": "ActivePath"
                }
            ],
            "Default": "DefaultPath"
        });
        let input = json!({"status": "unknown"});
        assert_eq!(
            evaluate_choice(&state_def, &input, None).unwrap(),
            Some("DefaultPath".to_string())
        );
    }

    #[test]
    fn test_evaluate_choice_matching() {
        let state_def = json!({
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.value",
                    "NumericGreaterThan": 100,
                    "Next": "High"
                },
                {
                    "Variable": "$.value",
                    "NumericLessThanEquals": 100,
                    "Next": "Low"
                }
            ],
            "Default": "Unknown"
        });
        let input = json!({"value": 150});
        assert_eq!(
            evaluate_choice(&state_def, &input, None).unwrap(),
            Some("High".to_string())
        );

        let input = json!({"value": 50});
        assert_eq!(
            evaluate_choice(&state_def, &input, None).unwrap(),
            Some("Low".to_string())
        );
    }

    #[test]
    fn test_evaluate_choice_no_match_no_default() {
        let state_def = json!({
            "Type": "Choice",
            "Choices": [
                {
                    "Variable": "$.status",
                    "StringEquals": "active",
                    "Next": "Active"
                }
            ]
        });
        let input = json!({"status": "closed"});
        assert_eq!(evaluate_choice(&state_def, &input, None).unwrap(), None);
    }

    #[test]
    fn test_numeric_equals_path() {
        let rule = json!({
            "Variable": "$.a",
            "NumericEqualsPath": "$.b",
            "Next": "Equal"
        });
        let input = json!({"a": 42, "b": 42});
        assert!(evaluate_rule(&rule, &input, None).unwrap());

        let input = json!({"a": 42, "b": 99});
        assert!(!evaluate_rule(&rule, &input, None).unwrap());
    }

    #[test]
    fn test_timestamp_comparisons() {
        let rule = json!({
            "Variable": "$.ts",
            "TimestampLessThan": "2024-06-01T00:00:00Z",
            "Next": "Before"
        });
        let input = json!({"ts": "2024-01-15T12:00:00Z"});
        assert!(evaluate_rule(&rule, &input, None).unwrap());

        let input = json!({"ts": "2024-12-01T00:00:00Z"});
        assert!(!evaluate_rule(&rule, &input, None).unwrap());
    }

    #[test]
    fn test_string_less_than() {
        let rule = json!({
            "Variable": "$.name",
            "StringLessThan": "beta",
            "Next": "Before"
        });
        let input = json!({"name": "alpha"});
        assert!(evaluate_rule(&rule, &input, None).unwrap());

        let input = json!({"name": "gamma"});
        assert!(!evaluate_rule(&rule, &input, None).unwrap());
    }

    fn rule_ok(rule: Value, input: Value) -> bool {
        evaluate_rule(&rule, &input, None).unwrap()
    }

    #[test]
    fn every_path_comparator() {
        let input = json!({
            "s": "b", "s_lo": "a", "s_hi": "c", "s_eq": "b",
            "n": 5, "n_lo": 1, "n_hi": 9.5, "n_eq": 5.0,
            "b": true, "b_eq": true,
            "t": "2024-06-01T00:00:00Z", "t_lo": "2024-01-01T00:00:00Z",
            "t_hi": "2025-01-01T00:00:00+02:00", "t_eq": "2024-06-01T02:00:00+02:00",
            "pat": "b*"
        });
        let cases: &[(&str, &str, &str, bool)] = &[
            ("$.s", "StringEqualsPath", "$.s_eq", true),
            ("$.s", "StringEqualsPath", "$.s_lo", false),
            ("$.s", "StringLessThanPath", "$.s_hi", true),
            ("$.s", "StringLessThanPath", "$.s_lo", false),
            ("$.s", "StringGreaterThanPath", "$.s_lo", true),
            ("$.s", "StringLessThanEqualsPath", "$.s_eq", true),
            ("$.s", "StringGreaterThanEqualsPath", "$.s_hi", false),
            ("$.s", "StringMatchesPath", "$.pat", true),
            ("$.n", "NumericEqualsPath", "$.n_eq", true),
            ("$.n", "NumericLessThanPath", "$.n_hi", true),
            ("$.n", "NumericGreaterThanPath", "$.n_lo", true),
            ("$.n", "NumericLessThanEqualsPath", "$.n_eq", true),
            ("$.n", "NumericGreaterThanEqualsPath", "$.n_hi", false),
            ("$.b", "BooleanEqualsPath", "$.b_eq", true),
            ("$.t", "TimestampEqualsPath", "$.t_eq", true),
            ("$.t", "TimestampLessThanPath", "$.t_hi", true),
            ("$.t", "TimestampGreaterThanPath", "$.t_lo", true),
            ("$.t", "TimestampLessThanEqualsPath", "$.t_eq", true),
            ("$.t", "TimestampGreaterThanEqualsPath", "$.t_hi", false),
            // Type mismatch is false, not an error.
            ("$.s", "NumericEqualsPath", "$.n", false),
        ];
        for (var, op, path, expected) in cases {
            let rule = json!({"Variable": var, *op: path, "Next": "X"});
            assert_eq!(rule_ok(rule, input.clone()), *expected, "{op} {path}");
        }
    }

    #[test]
    fn missing_variable_is_states_runtime() {
        let rule = json!({"Variable": "$.missing", "StringEquals": "x", "Next": "X"});
        let (err, cause) = evaluate_rule(&rule, &json!({}), None).unwrap_err();
        assert_eq!(err, "States.Runtime");
        assert!(cause.contains("$.missing"), "{cause}");
        // Missing *Path comparand fails too.
        let rule = json!({"Variable": "$.a", "NumericEqualsPath": "$.nope", "Next": "X"});
        assert!(evaluate_rule(&rule, &json!({"a": 1}), None).is_err());
        // Type tests on a missing field fail; only IsPresent may probe it.
        let rule = json!({"Variable": "$.missing", "IsNull": true, "Next": "X"});
        assert!(evaluate_rule(&rule, &json!({}), None).is_err());
        let state = json!({"Choices": [rule], "Default": "D"});
        assert!(evaluate_choice(&state, &json!({}), None).is_err());
    }

    #[test]
    fn is_present_guards_missing_variable() {
        let rule = json!({
            "And": [
                {"Variable": "$.x", "IsPresent": true},
                {"Variable": "$.x", "StringEquals": "y"}
            ],
            "Next": "X"
        });
        assert!(!rule_ok(rule.clone(), json!({})));
        assert!(rule_ok(rule, json!({"x": "y"})));
        let rule = json!({"Variable": "$.items[3]", "IsPresent": false, "Next": "X"});
        assert!(rule_ok(rule, json!({"items": [1]})));
    }

    #[test]
    fn type_tests() {
        let t = |op: &str, v: Value| {
            rule_ok(
                json!({"Variable": "$.v", op: true, "Next": "X"}),
                json!({"v": v}),
            )
        };
        assert!(t("IsString", json!("a")));
        assert!(!t("IsString", json!(1)));
        assert!(t("IsBoolean", json!(false)));
        assert!(t("IsNumeric", json!(1.5)));
        assert!(t("IsNull", Value::Null));
        assert!(t("IsTimestamp", json!("2024-01-01T00:00:00Z")));
        assert!(!t("IsTimestamp", json!("yesterday")));
    }

    #[test]
    fn string_matches_escapes() {
        assert!(string_matches("a*b", "a\\*b"));
        assert!(!string_matches("axb", "a\\*b"));
        assert!(string_matches("a\\b", "a\\\\b"));
        assert!(string_matches("a\\bc", "a\\\\*"));
    }

    #[test]
    fn variable_may_read_context_object() {
        let ctx = json!({"Execution": {"Name": "run-1"}});
        let rule = json!({"Variable": "$$.Execution.Name", "StringEquals": "run-1", "Next": "X"});
        assert!(evaluate_rule(&rule, &json!({}), Some(&ctx)).unwrap());
        let rule = json!({"Variable": "$$.Nope", "IsPresent": false, "Next": "X"});
        assert!(evaluate_rule(&rule, &json!({}), Some(&ctx)).unwrap());
    }
}
