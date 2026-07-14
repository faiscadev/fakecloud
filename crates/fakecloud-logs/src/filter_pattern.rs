//! CloudWatch Logs filter pattern evaluator used by metric filters.
//!
//! Supports the documented variants:
//! - Empty pattern matches everything.
//! - Quoted phrase: substring match of the literal text inside the quotes.
//! - Plain string (single token): substring match.
//! - Space-separated terms: AND match (all terms must appear in the message).
//! - JSON pattern `{ <expr> }`: parses the message as JSON, evaluates a
//!   boolean expression with `=`, `!=`, `>`, `<`, `>=`, `<=`, `&&`, `||`.
//!
//! Independent from the simpler `matches_filter_pattern` helper in
//! `service/mod.rs`; metric filters need `||` plus JSON-path value
//! extraction for `MetricValue = $.field`, which the older helper
//! doesn't expose.
//!
//! Array-style patterns (`[a, b, c]`) match positionally against
//! whitespace-separated tokens in the message. Each comma-separated
//! field is either a bare name (always matches that slot), a quoted
//! string, a literal value for equality, or a comparison `=v`,
//! `!=v`, `>=v`, `<=v`, `>v`, `<v`. `...` is a wildcard that skips any
//! number of tokens.
//!
//! `IS NULL` and free-form regex are out of scope and fail closed.

use serde_json::Value;

/// Returns true when `message` matches the given filter pattern.
pub fn matches(pattern: &str, message: &str) -> bool {
    let pattern = pattern.trim();
    if pattern.is_empty() {
        return true;
    }

    if pattern.starts_with('{') && pattern.ends_with('}') {
        return matches_json(pattern, message);
    }

    if pattern.starts_with('[') && pattern.ends_with(']') {
        return matches_array(pattern, message);
    }
    if pattern.starts_with('[') {
        return false;
    }

    if pattern.starts_with('"') && pattern.ends_with('"') && pattern.len() >= 2 {
        let inner = &pattern[1..pattern.len() - 1];
        let unescaped = inner.replace("\\\"", "\"");
        return message.contains(&unescaped);
    }

    let terms = tokenize(pattern);
    if terms.is_empty() {
        return true;
    }
    terms.iter().all(|t| message.contains(t.as_str()))
}

/// Resolve the literal `MetricValue` from a metric filter transformation.
///
/// `metric_value` is either:
/// - a number literal (e.g. `"1"`, `"42.5"`),
/// - a JSON path reference (e.g. `"$.bytes"`) extracted from the matched
///   message,
/// - empty/missing, falling back to `default_value` or `1.0`.
pub fn resolve_metric_value(metric_value: &str, default_value: Option<f64>, message: &str) -> f64 {
    let trimmed = metric_value.trim();
    if trimmed.is_empty() {
        return default_value.unwrap_or(1.0);
    }

    if let Some(path) = trimmed.strip_prefix("$.") {
        if let Ok(json) = serde_json::from_str::<Value>(message) {
            if let Some(v) = resolve_path(&json, path) {
                if let Some(n) = v.as_f64() {
                    return n;
                }
                if let Some(s) = v.as_str() {
                    if let Ok(n) = s.parse::<f64>() {
                        return n;
                    }
                }
            }
        }
        return default_value.unwrap_or(1.0);
    }

    trimmed
        .parse::<f64>()
        .unwrap_or_else(|_| default_value.unwrap_or(1.0))
}

fn matches_json(pattern: &str, message: &str) -> bool {
    let inner = pattern
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .unwrap_or("")
        .trim();
    if inner.is_empty() {
        return true;
    }

    let json: Value = match serde_json::from_str(message) {
        Ok(v) => v,
        Err(_) => return false,
    };

    eval_or(inner, &json)
}

/// Top-level disjunction: `a || b || c` -> any of `a`, `b`, `c`.
fn eval_or(expr: &str, json: &Value) -> bool {
    split_top_level(expr, "||")
        .into_iter()
        .any(|chunk| eval_and(chunk.trim(), json))
}

/// Conjunction below `||`: `a && b` -> all of `a`, `b`.
fn eval_and(expr: &str, json: &Value) -> bool {
    split_top_level(expr, "&&")
        .into_iter()
        .all(|chunk| eval_atom(chunk.trim(), json))
}

/// Split `expr` on `sep`, ignoring occurrences inside quoted strings.
fn split_top_level(expr: &str, sep: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let bytes = expr.as_bytes();
    let sep_bytes = sep.as_bytes();
    let mut start = 0usize;
    let mut i = 0usize;
    let mut in_quotes = false;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'\\' && i + 1 < bytes.len() {
            i += 2;
            continue;
        }
        if c == b'"' {
            in_quotes = !in_quotes;
            i += 1;
            continue;
        }
        if !in_quotes && bytes[i..].starts_with(sep_bytes) {
            parts.push(expr[start..i].to_string());
            i += sep_bytes.len();
            start = i;
            continue;
        }
        i += 1;
    }
    parts.push(expr[start..].to_string());
    parts
}

fn eval_atom(condition: &str, json: &Value) -> bool {
    let condition = condition.trim();
    let condition = condition
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .map(|s| s.trim())
        .unwrap_or(condition);

    let ops = ["!=", ">=", "<=", "=", ">", "<"];
    let mut found: Option<(&str, usize)> = None;
    // Iterate by character index so a multi-byte character (e.g. `é`) never
    // leaves the cursor on a UTF-8 continuation byte, which would panic when
    // slicing `condition[i..]`. The operator tokens are all ASCII.
    let mut in_quotes = false;
    let mut escaped = false;
    for (i, c) in condition.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if c == '\\' {
            escaped = true;
            continue;
        }
        if c == '"' {
            in_quotes = !in_quotes;
            continue;
        }
        if !in_quotes {
            if let Some(op) = ops
                .iter()
                .find(|op| condition[i..].starts_with(*op))
                .copied()
            {
                found = Some((op, i));
                break;
            }
        }
    }

    let Some((op, pos)) = found else {
        // No comparison: `{ $.field }` matches when field exists.
        if let Some(path) = condition.strip_prefix("$.") {
            return resolve_path(json, path).is_some();
        }
        return false;
    };

    let field = condition[..pos].trim();
    let value = condition[pos + op.len()..].trim();

    let path = match field.strip_prefix("$.") {
        Some(p) => p,
        None => return false,
    };

    let actual = match resolve_path(json, path) {
        Some(v) => v,
        // Missing field: only `!=` semantically holds.
        None => return op == "!=",
    };

    if value.starts_with('"') && value.ends_with('"') && value.len() >= 2 {
        let s = &value[1..value.len() - 1];
        let unescaped = s.replace("\\\"", "\"");
        return match op {
            "=" => actual.as_str() == Some(unescaped.as_str()),
            "!=" => actual.as_str() != Some(unescaped.as_str()),
            _ => false,
        };
    }

    if let Ok(num) = value.parse::<f64>() {
        let actual_num = actual.as_f64();
        return match (op, actual_num) {
            ("=", Some(n)) => (n - num).abs() < f64::EPSILON,
            ("!=", Some(n)) => (n - num).abs() >= f64::EPSILON,
            (">", Some(n)) => n > num,
            ("<", Some(n)) => n < num,
            (">=", Some(n)) => n >= num,
            ("<=", Some(n)) => n <= num,
            _ => false,
        };
    }

    if value == "true" || value == "false" {
        let expected = value == "true";
        return match op {
            "=" => actual.as_bool() == Some(expected),
            "!=" => actual.as_bool() != Some(expected),
            _ => false,
        };
    }

    false
}

fn matches_array(pattern: &str, message: &str) -> bool {
    let inner = pattern
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or("")
        .trim();
    if inner.is_empty() {
        return true;
    }
    let fields: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
    let tokens: Vec<&str> = message.split_whitespace().collect();
    array_match(&fields, &tokens)
}

fn array_match(fields: &[&str], tokens: &[&str]) -> bool {
    if fields.is_empty() {
        return tokens.is_empty();
    }
    let head = fields[0];
    if head == "..." {
        let rest = &fields[1..];
        if rest.is_empty() {
            return true;
        }
        for i in 0..=tokens.len() {
            if array_match(rest, &tokens[i..]) {
                return true;
            }
        }
        return false;
    }
    if tokens.is_empty() {
        return false;
    }
    if !array_field_matches(head, tokens[0]) {
        return false;
    }
    array_match(&fields[1..], &tokens[1..])
}

fn array_field_matches(field: &str, token: &str) -> bool {
    let f = field.trim();
    if f == "*" || f.is_empty() {
        return true;
    }
    // Quoted literal -> equality.
    if f.starts_with('"') && f.ends_with('"') && f.len() >= 2 {
        return token == &f[1..f.len() - 1];
    }
    // Comparison ops with no LHS (`=200`, `>=400`, etc.) compare against the token.
    for op in ["!=", ">=", "<=", "=", ">", "<"] {
        if let Some(rhs) = f.strip_prefix(op) {
            return cmp_field(op, token, rhs.trim());
        }
    }
    // `name=value` style (used in named-array patterns).
    for op in ["!=", ">=", "<=", "=", ">", "<"] {
        if let Some(idx) = f.find(op) {
            let rhs = &f[idx + op.len()..].trim();
            return cmp_field(op, token, rhs);
        }
    }
    // Bare identifier -> name placeholder, matches anything.
    if f.chars()
        .next()
        .is_some_and(|c| c.is_alphabetic() || c == '_' || c == '$')
    {
        return true;
    }
    // Fallback: treat as literal.
    token == f
}

fn cmp_field(op: &str, token: &str, rhs: &str) -> bool {
    let rhs = rhs.trim();
    let rhs = if rhs.starts_with('"') && rhs.ends_with('"') && rhs.len() >= 2 {
        &rhs[1..rhs.len() - 1]
    } else {
        rhs
    };
    if let (Ok(a), Ok(b)) = (token.parse::<f64>(), rhs.parse::<f64>()) {
        return match op {
            "=" => (a - b).abs() < f64::EPSILON,
            "!=" => (a - b).abs() >= f64::EPSILON,
            ">" => a > b,
            "<" => a < b,
            ">=" => a >= b,
            "<=" => a <= b,
            _ => false,
        };
    }
    match op {
        "=" => token == rhs,
        "!=" => token != rhs,
        _ => false,
    }
}

fn resolve_path<'a>(json: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = json;
    for part in path.split('.') {
        current = current.get(part)?;
    }
    if current.is_null() {
        None
    } else {
        Some(current)
    }
}

fn tokenize(pattern: &str) -> Vec<String> {
    let mut terms = Vec::new();
    let mut chars = pattern.chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
            continue;
        }
        if c == '"' {
            chars.next();
            let mut buf = String::new();
            loop {
                match chars.next() {
                    Some('\\') => {
                        if let Some(n) = chars.next() {
                            buf.push(n);
                        }
                    }
                    Some('"') => break,
                    Some(ch) => buf.push(ch),
                    None => break,
                }
            }
            terms.push(buf);
        } else {
            let mut buf = String::new();
            while let Some(&ch) = chars.peek() {
                if ch.is_whitespace() {
                    break;
                }
                buf.push(ch);
                chars.next();
            }
            if !buf.is_empty() {
                terms.push(buf);
            }
        }
    }
    terms
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_string_pattern_matches_substring() {
        assert!(matches("ERROR", "service ERROR: timeout"));
        assert!(!matches("ERROR", "service INFO: ok"));
    }

    #[test]
    fn quoted_phrase_pattern_matches_exact() {
        assert!(matches(
            "\"connection refused\"",
            "tcp: connection refused on :8080"
        ));
        assert!(!matches(
            "\"connection refused\"",
            "tcp: connection was refused"
        ));
    }

    #[test]
    fn space_separated_terms_require_all_to_match() {
        assert!(matches("ERROR DATABASE", "ERROR: DATABASE down"));
        assert!(!matches("ERROR DATABASE", "ERROR: cache miss"));
        assert!(!matches("ERROR DATABASE", "INFO: DATABASE healthy"));
    }

    #[test]
    fn json_pattern_equals_predicate() {
        assert!(matches("{ $.statusCode = 500 }", r#"{"statusCode": 500}"#));
        assert!(!matches("{ $.statusCode = 500 }", r#"{"statusCode": 200}"#));
    }

    #[test]
    fn json_pattern_inequality_predicate() {
        assert!(matches("{ $.statusCode != 200 }", r#"{"statusCode": 500}"#));
        assert!(!matches(
            "{ $.statusCode != 200 }",
            r#"{"statusCode": 200}"#
        ));
    }

    #[test]
    fn json_pattern_and_predicate() {
        let p = "{ $.statusCode = 500 && $.method = \"GET\" }";
        assert!(matches(p, r#"{"statusCode": 500, "method": "GET"}"#));
        assert!(!matches(p, r#"{"statusCode": 500, "method": "POST"}"#));
        assert!(!matches(p, r#"{"statusCode": 200, "method": "GET"}"#));
    }

    #[test]
    fn json_pattern_or_predicate() {
        let p = "{ $.statusCode = 500 || $.statusCode = 503 }";
        assert!(matches(p, r#"{"statusCode": 500}"#));
        assert!(matches(p, r#"{"statusCode": 503}"#));
        assert!(!matches(p, r#"{"statusCode": 200}"#));
    }

    #[test]
    fn json_pattern_numeric_comparisons() {
        assert!(matches("{ $.latency > 100 }", r#"{"latency": 250}"#));
        assert!(!matches("{ $.latency > 100 }", r#"{"latency": 50}"#));
        assert!(matches("{ $.latency <= 100 }", r#"{"latency": 100}"#));
    }

    #[test]
    fn json_pattern_against_non_json_message_fails() {
        assert!(!matches("{ $.statusCode = 500 }", "plain text, not JSON"));
    }

    #[test]
    fn empty_pattern_matches_anything() {
        assert!(matches("", "anything"));
        assert!(matches("   ", "anything"));
    }

    #[test]
    fn array_pattern_positional_match() {
        assert!(matches("[a, b]", "a b"));
        assert!(matches("[host, status]", "192.168.1.1 200"));
        assert!(!matches("[a, b, c]", "a b"));
    }

    #[test]
    fn array_pattern_comparison_ops() {
        assert!(matches("[ip, =200]", "1.2.3.4 200"));
        assert!(!matches("[ip, =200]", "1.2.3.4 500"));
        assert!(matches("[ip, >=400]", "1.2.3.4 500"));
        assert!(!matches("[ip, >=400]", "1.2.3.4 200"));
        assert!(matches("[ip, !=200]", "1.2.3.4 500"));
    }

    #[test]
    fn array_pattern_named_field_with_predicate() {
        assert!(matches("[ip, status=200]", "1.2.3.4 200"));
        assert!(!matches("[ip, status=200]", "1.2.3.4 404"));
    }

    #[test]
    fn array_pattern_ellipsis_skips_tokens() {
        assert!(matches("[ip, ..., status]", "1.2.3.4 a b c 200"));
        assert!(matches("[..., =200]", "any tokens 200"));
        assert!(!matches("[..., =500]", "any tokens 200"));
    }

    #[test]
    fn array_pattern_quoted_literal() {
        assert!(matches("[\"GET\", path]", "GET /index.html"));
        assert!(!matches("[\"POST\", path]", "GET /index.html"));
    }

    #[test]
    fn array_pattern_unbalanced_fails_closed() {
        assert!(!matches("[a, b", "a b"));
    }

    #[test]
    fn resolve_metric_value_literal_number() {
        assert_eq!(resolve_metric_value("1", None, "msg"), 1.0);
        assert_eq!(resolve_metric_value("42.5", None, "msg"), 42.5);
    }

    #[test]
    fn resolve_metric_value_json_path_extracts_field() {
        let v = resolve_metric_value("$.bytes", None, r#"{"bytes": 1024}"#);
        assert_eq!(v, 1024.0);
    }

    #[test]
    fn multibyte_json_pattern_does_not_panic() {
        // A multi-byte character in the JSON condition must not panic the
        // byte-index scan (regression: non-char-boundary slice).
        assert!(!matches("{ café }", r#"{"statusCode": 500}"#));
        assert!(!matches("{ $.café = 1 }", r#"{"statusCode": 500}"#));
        assert!(matches("{ $.café = 1 }", r#"{"café": 1}"#));
        // Multi-byte inside a quoted value must also be safe.
        assert!(matches("{ $.name = \"café\" }", r#"{"name": "café"}"#));
    }

    #[test]
    fn resolve_metric_value_falls_back_when_missing() {
        let v = resolve_metric_value("$.bytes", Some(7.0), r#"{"other": 1}"#);
        assert_eq!(v, 7.0);
        let v = resolve_metric_value("", None, "msg");
        assert_eq!(v, 1.0);
    }
}
