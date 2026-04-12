use serde_json::Value;

use crate::io_processing::resolve_path;

/// Evaluate a Choice state's rules against the input and return the Next state name.
/// Returns None if no rule matches and there's no Default.
pub fn evaluate_choice(state_def: &Value, input: &Value) -> Option<String> {
    if let Some(choices) = state_def["Choices"].as_array() {
        for choice in choices {
            if evaluate_rule(choice, input) {
                return choice["Next"].as_str().map(|s| s.to_string());
            }
        }
    }

    // Fall through to Default
    state_def["Default"].as_str().map(|s| s.to_string())
}

/// Evaluate a single choice rule (may be compound via And/Or/Not).
fn evaluate_rule(rule: &Value, input: &Value) -> bool {
    // Logical operators
    if let Some(and_rules) = rule["And"].as_array() {
        return and_rules.iter().all(|r| evaluate_rule(r, input));
    }
    if let Some(or_rules) = rule["Or"].as_array() {
        return or_rules.iter().any(|r| evaluate_rule(r, input));
    }
    if rule.get("Not").is_some() {
        return !evaluate_rule(&rule["Not"], input);
    }

    // Get the variable value
    let variable = match rule["Variable"].as_str() {
        Some(v) => v,
        None => return false,
    };
    let value = resolve_path(input, variable);

    // Presence/type checks
    if let Some(expected) = rule.get("IsPresent") {
        // Check if the field exists in the input (including explicit null).
        // resolve_path returns Value::Null for both missing and null fields,
        // so we need to check the parent object directly.
        let is_present = field_exists_in_input(input, variable);
        return expected.as_bool().unwrap_or(false) == is_present;
    }
    if let Some(expected) = rule.get("IsNull") {
        let is_null = value.is_null();
        return expected.as_bool().unwrap_or(false) == is_null;
    }
    if let Some(expected) = rule.get("IsNumeric") {
        let is_numeric = value.is_number();
        return expected.as_bool().unwrap_or(false) == is_numeric;
    }
    if let Some(expected) = rule.get("IsString") {
        let is_string = value.is_string();
        return expected.as_bool().unwrap_or(false) == is_string;
    }
    if let Some(expected) = rule.get("IsBoolean") {
        let is_boolean = value.is_boolean();
        return expected.as_bool().unwrap_or(false) == is_boolean;
    }
    if let Some(expected) = rule.get("IsTimestamp") {
        let is_ts = value
            .as_str()
            .map(|s| chrono::DateTime::parse_from_rfc3339(s).is_ok())
            .unwrap_or(false);
        return expected.as_bool().unwrap_or(false) == is_ts;
    }

    // String comparisons
    if let Some(expected) = rule["StringEquals"].as_str() {
        return value.as_str() == Some(expected);
    }
    if let Some(path) = rule["StringEqualsPath"].as_str() {
        let other = resolve_path(input, path);
        return value.as_str().is_some() && value.as_str() == other.as_str();
    }
    if let Some(expected) = rule["StringLessThan"].as_str() {
        return value.as_str().is_some_and(|v| v < expected);
    }
    if let Some(expected) = rule["StringGreaterThan"].as_str() {
        return value.as_str().is_some_and(|v| v > expected);
    }
    if let Some(expected) = rule["StringLessThanEquals"].as_str() {
        return value.as_str().is_some_and(|v| v <= expected);
    }
    if let Some(expected) = rule["StringGreaterThanEquals"].as_str() {
        return value.as_str().is_some_and(|v| v >= expected);
    }
    if let Some(pattern) = rule["StringMatches"].as_str() {
        return value.as_str().is_some_and(|v| string_matches(v, pattern));
    }

    // Numeric comparisons
    if let Some(expected) = rule["NumericEquals"].as_f64() {
        return value.as_f64() == Some(expected);
    }
    if let Some(path) = rule["NumericEqualsPath"].as_str() {
        let other = resolve_path(input, path);
        return value.as_f64().is_some() && value.as_f64() == other.as_f64();
    }
    if let Some(expected) = rule["NumericLessThan"].as_f64() {
        return value.as_f64().is_some_and(|v| v < expected);
    }
    if let Some(expected) = rule["NumericGreaterThan"].as_f64() {
        return value.as_f64().is_some_and(|v| v > expected);
    }
    if let Some(expected) = rule["NumericLessThanEquals"].as_f64() {
        return value.as_f64().is_some_and(|v| v <= expected);
    }
    if let Some(expected) = rule["NumericGreaterThanEquals"].as_f64() {
        return value.as_f64().is_some_and(|v| v >= expected);
    }

    // Boolean comparisons
    if let Some(expected) = rule["BooleanEquals"].as_bool() {
        return value.as_bool() == Some(expected);
    }
    if let Some(path) = rule["BooleanEqualsPath"].as_str() {
        let other = resolve_path(input, path);
        return value.as_bool().is_some() && value.as_bool() == other.as_bool();
    }

    // Timestamp comparisons
    if let Some(expected) = rule["TimestampEquals"].as_str() {
        return compare_timestamps(&value, expected, |a, b| a == b);
    }
    if let Some(expected) = rule["TimestampLessThan"].as_str() {
        return compare_timestamps(&value, expected, |a, b| a < b);
    }
    if let Some(expected) = rule["TimestampGreaterThan"].as_str() {
        return compare_timestamps(&value, expected, |a, b| a > b);
    }
    if let Some(expected) = rule["TimestampLessThanEquals"].as_str() {
        return compare_timestamps(&value, expected, |a, b| a <= b);
    }
    if let Some(expected) = rule["TimestampGreaterThanEquals"].as_str() {
        return compare_timestamps(&value, expected, |a, b| a >= b);
    }

    false
}

/// Compare two RFC3339 timestamps using the provided comparison function.
fn compare_timestamps<F>(value: &Value, expected: &str, cmp: F) -> bool
where
    F: Fn(chrono::DateTime<chrono::FixedOffset>, chrono::DateTime<chrono::FixedOffset>) -> bool,
{
    let val_str = match value.as_str() {
        Some(s) => s,
        None => return false,
    };
    let val_ts = match chrono::DateTime::parse_from_rfc3339(val_str) {
        Ok(t) => t,
        Err(_) => return false,
    };
    let exp_ts = match chrono::DateTime::parse_from_rfc3339(expected) {
        Ok(t) => t,
        Err(_) => return false,
    };
    cmp(val_ts, exp_ts)
}

/// Glob-style pattern matching for StringMatches.
/// Supports `*` (matches any sequence) and `\*` (literal asterisk).
fn string_matches(value: &str, pattern: &str) -> bool {
    let mut pattern_chars: Vec<char> = pattern.chars().collect();
    let value_chars: Vec<char> = value.chars().collect();

    // Preprocess: handle escaped asterisks
    let mut segments: Vec<PatternSegment> = Vec::new();
    let mut current = String::new();
    let mut i = 0;
    while i < pattern_chars.len() {
        if pattern_chars[i] == '\\' && i + 1 < pattern_chars.len() && pattern_chars[i + 1] == '*' {
            current.push('*');
            i += 2;
        } else if pattern_chars[i] == '*' {
            if !current.is_empty() {
                segments.push(PatternSegment::Literal(current.clone()));
                current.clear();
            }
            segments.push(PatternSegment::Wildcard);
            i += 1;
        } else {
            current.push(pattern_chars[i]);
            i += 1;
        }
    }
    if !current.is_empty() {
        segments.push(PatternSegment::Literal(current));
    }

    // Use cleaned-up pattern_chars for matching
    pattern_chars = Vec::new();
    for seg in &segments {
        match seg {
            PatternSegment::Literal(s) => {
                for c in s.chars() {
                    pattern_chars.push(c);
                }
            }
            PatternSegment::Wildcard => {
                pattern_chars.push('\0'); // sentinel for wildcard
            }
        }
    }

    // DP matching
    let m = value_chars.len();
    let n = pattern_chars.len();
    let mut dp = vec![vec![false; n + 1]; m + 1];
    dp[0][0] = true;

    // Handle leading wildcards
    for j in 1..=n {
        if pattern_chars[j - 1] == '\0' {
            dp[0][j] = dp[0][j - 1];
        }
    }

    for i in 1..=m {
        for j in 1..=n {
            if pattern_chars[j - 1] == '\0' {
                dp[i][j] = dp[i][j - 1] || dp[i - 1][j];
            } else if pattern_chars[j - 1] == value_chars[i - 1] {
                dp[i][j] = dp[i - 1][j - 1];
            }
        }
    }

    dp[m][n]
}

/// Check if a field referenced by a JsonPath expression actually exists in the input,
/// including fields explicitly set to null. This is different from resolve_path which
/// returns Value::Null for both missing and null fields.
fn field_exists_in_input(root: &Value, path: &str) -> bool {
    if path == "$" {
        return true;
    }
    let path = path.strip_prefix("$.").unwrap_or(path);
    let segments: Vec<&str> = path.split('.').collect();
    let mut current = root;

    for (i, segment) in segments.iter().enumerate() {
        if i == segments.len() - 1 {
            // Last segment — check if it exists (even if null)
            return match current.as_object() {
                Some(obj) => obj.contains_key(*segment),
                None => false,
            };
        } else {
            match current.get(*segment) {
                Some(v) => current = v,
                None => return false,
            }
        }
    }
    false
}

enum PatternSegment {
    Literal(String),
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
        assert!(evaluate_rule(&rule, &input));

        let input = json!({"status": "inactive"});
        assert!(!evaluate_rule(&rule, &input));
    }

    #[test]
    fn test_numeric_greater_than() {
        let rule = json!({
            "Variable": "$.count",
            "NumericGreaterThan": 10,
            "Next": "High"
        });
        let input = json!({"count": 15});
        assert!(evaluate_rule(&rule, &input));

        let input = json!({"count": 5});
        assert!(!evaluate_rule(&rule, &input));
    }

    #[test]
    fn test_boolean_equals() {
        let rule = json!({
            "Variable": "$.enabled",
            "BooleanEquals": true,
            "Next": "Enabled"
        });
        let input = json!({"enabled": true});
        assert!(evaluate_rule(&rule, &input));

        let input = json!({"enabled": false});
        assert!(!evaluate_rule(&rule, &input));
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
        assert!(evaluate_rule(&rule, &input));

        let input = json!({"a": -1, "b": 50});
        assert!(!evaluate_rule(&rule, &input));
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
        assert!(evaluate_rule(&rule, &input));

        let input = json!({"status": "closed"});
        assert!(!evaluate_rule(&rule, &input));
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
        assert!(evaluate_rule(&rule, &input));

        let input = json!({"status": "closed"});
        assert!(!evaluate_rule(&rule, &input));
    }

    #[test]
    fn test_is_present() {
        let rule = json!({
            "Variable": "$.optional",
            "IsPresent": true,
            "Next": "HasField"
        });
        let input = json!({"optional": "value"});
        assert!(evaluate_rule(&rule, &input));

        let input = json!({"other": "value"});
        assert!(!evaluate_rule(&rule, &input));
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
        assert!(evaluate_rule(&rule, &input));
    }

    #[test]
    fn test_is_null() {
        let rule = json!({
            "Variable": "$.field",
            "IsNull": true,
            "Next": "Null"
        });
        let input = json!({"field": null});
        assert!(evaluate_rule(&rule, &input));

        let input = json!({"field": "value"});
        assert!(!evaluate_rule(&rule, &input));
    }

    #[test]
    fn test_is_numeric() {
        let rule = json!({
            "Variable": "$.value",
            "IsNumeric": true,
            "Next": "Number"
        });
        let input = json!({"value": 42});
        assert!(evaluate_rule(&rule, &input));

        let input = json!({"value": "not a number"});
        assert!(!evaluate_rule(&rule, &input));
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
            evaluate_choice(&state_def, &input),
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
            evaluate_choice(&state_def, &input),
            Some("High".to_string())
        );

        let input = json!({"value": 50});
        assert_eq!(evaluate_choice(&state_def, &input), Some("Low".to_string()));
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
        assert_eq!(evaluate_choice(&state_def, &input), None);
    }

    #[test]
    fn test_numeric_equals_path() {
        let rule = json!({
            "Variable": "$.a",
            "NumericEqualsPath": "$.b",
            "Next": "Equal"
        });
        let input = json!({"a": 42, "b": 42});
        assert!(evaluate_rule(&rule, &input));

        let input = json!({"a": 42, "b": 99});
        assert!(!evaluate_rule(&rule, &input));
    }

    #[test]
    fn test_timestamp_comparisons() {
        let rule = json!({
            "Variable": "$.ts",
            "TimestampLessThan": "2024-06-01T00:00:00Z",
            "Next": "Before"
        });
        let input = json!({"ts": "2024-01-15T12:00:00Z"});
        assert!(evaluate_rule(&rule, &input));

        let input = json!({"ts": "2024-12-01T00:00:00Z"});
        assert!(!evaluate_rule(&rule, &input));
    }

    #[test]
    fn test_string_less_than() {
        let rule = json!({
            "Variable": "$.name",
            "StringLessThan": "beta",
            "Next": "Before"
        });
        let input = json!({"name": "alpha"});
        assert!(evaluate_rule(&rule, &input));

        let input = json!({"name": "gamma"});
        assert!(!evaluate_rule(&rule, &input));
    }
}
