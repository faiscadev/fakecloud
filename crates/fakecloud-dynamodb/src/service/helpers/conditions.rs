//! dynamodb helpers `conditions` concerns (audit-2026-05-19).

use super::*;

pub(crate) fn evaluate_condition(
    condition: &str,
    existing: Option<&HashMap<String, AttributeValue>>,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> Result<(), AwsServiceError> {
    // ConditionExpression and FilterExpression share the same DynamoDB grammar,
    // so we delegate to evaluate_filter_expression. An empty map models "item
    // doesn't exist" correctly: attribute_exists → false, attribute_not_exists
    // → true, comparisons against missing attributes → None vs Some(val).
    let empty = HashMap::new();
    let item = existing.unwrap_or(&empty);
    if evaluate_filter_expression(condition, item, expr_attr_names, expr_attr_values) {
        Ok(())
    } else {
        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ConditionalCheckFailedException",
            "The conditional request failed",
        ))
    }
}

pub(crate) fn extract_function_arg<'a>(expr: &'a str, func_name: &str) -> Option<&'a str> {
    // aws-sdk-go v2's expression builder emits function calls with a space
    // between the name and the opening paren (`attribute_exists (#0)`),
    // while hand-written expressions usually don't — accept both.
    let with_paren = format!("{func_name}(");
    let with_space = format!("{func_name} (");
    let rest = expr
        .strip_prefix(&with_paren)
        .or_else(|| expr.strip_prefix(&with_space))?;
    let inner = rest.strip_suffix(')')?;
    Some(inner.trim())
}

pub(crate) fn evaluate_key_condition(
    expr: &str,
    item: &HashMap<String, AttributeValue>,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> bool {
    let trimmed = expr.trim();

    let parts = split_on_and(trimmed);
    if parts.len() > 1 {
        return parts.iter().all(|part| {
            evaluate_key_condition(part.trim(), item, expr_attr_names, expr_attr_values)
        });
    }

    let stripped = strip_outer_parens(trimmed);
    if stripped != trimmed {
        return evaluate_key_condition(stripped, item, expr_attr_names, expr_attr_values);
    }

    evaluate_single_key_condition(trimmed, item, expr_attr_names, expr_attr_values)
}

/// Split a DynamoDB condition expression on a top-level keyword (``AND`` /
/// ``OR``), case-insensitive, with ASCII-whitespace word boundaries so
/// ``:s\tAND\t:o`` and ``:s\nAND\n:o`` split the same as ``:s AND :o``.
///
/// Parenthesised groups are skipped so only unparenthesised occurrences of the
/// keyword act as separators. When splitting on ``AND``, each top-level
/// ``BETWEEN`` keyword consumes the next top-level ``AND`` as its own inner
/// separator (``x BETWEEN :lo AND :hi``) rather than letting it split the
/// expression.
pub(crate) fn split_on_top_level_keyword<'a>(expr: &'a str, keyword: &str) -> Vec<&'a str> {
    let bytes = expr.as_bytes();
    let len = bytes.len();
    let kw = keyword.as_bytes();
    let is_and = keyword.eq_ignore_ascii_case("AND");

    let mut parts: Vec<&str> = Vec::new();
    let mut start = 0usize;
    let mut depth: i32 = 0;
    let mut between_skip: u32 = 0;
    let mut i = 0usize;

    while i < len {
        let ch = bytes[i];
        if ch == b'(' {
            depth += 1;
            i += 1;
            continue;
        }
        if ch == b')' {
            if depth > 0 {
                depth -= 1;
            }
            i += 1;
            continue;
        }
        if depth == 0 {
            if is_and {
                if let Some(end) = match_keyword(bytes, i, b"BETWEEN") {
                    between_skip = between_skip.saturating_add(1);
                    i = end;
                    continue;
                }
            }
            if let Some(end) = match_keyword(bytes, i, kw) {
                if is_and && between_skip > 0 {
                    between_skip -= 1;
                    i = end;
                    continue;
                }
                parts.push(&expr[start..i]);
                start = end;
                i = end;
                continue;
            }
        }
        i += 1;
    }
    parts.push(&expr[start..]);
    parts
}

/// Case-insensitive keyword match. For alphanumeric keywords (``AND``,
/// ``OR``, ``BETWEEN``) the match also requires ASCII-whitespace word
/// boundaries so substrings of identifiers are not mistaken for keywords.
/// Punctuation keywords (``,``) match literally.
pub(crate) fn match_keyword(bytes: &[u8], i: usize, keyword: &[u8]) -> Option<usize> {
    let end = i + keyword.len();
    if end > bytes.len() {
        return None;
    }
    for k in 0..keyword.len() {
        if !bytes[i + k].eq_ignore_ascii_case(&keyword[k]) {
            return None;
        }
    }
    let needs_word_boundary = keyword.iter().all(|b| b.is_ascii_alphanumeric());
    if needs_word_boundary {
        let left_ok = i == 0 || bytes[i - 1].is_ascii_whitespace();
        if !left_ok {
            return None;
        }
        let right_ok = end == bytes.len() || bytes[end].is_ascii_whitespace();
        if !right_ok {
            return None;
        }
    }
    Some(end)
}

pub(crate) fn split_on_and(expr: &str) -> Vec<&str> {
    split_on_top_level_keyword(expr, "AND")
}

pub(crate) fn split_on_or(expr: &str) -> Vec<&str> {
    split_on_top_level_keyword(expr, "OR")
}

pub(crate) fn evaluate_single_key_condition(
    part: &str,
    item: &HashMap<String, AttributeValue>,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> bool {
    let part = part.trim();

    if let Some(rest) = part
        .strip_prefix("begins_with(")
        .or_else(|| part.strip_prefix("begins_with ("))
    {
        return key_cond_begins_with(rest, item, expr_attr_names, expr_attr_values);
    }

    // Require BETWEEN to sit on a word boundary so identifiers like
    // `my_between_attr` or placeholders containing `between` aren't
    // misparsed as range conditions.
    // Match `BETWEEN` only when bracketed by ASCII whitespace on both
    // sides so tabs / newlines / multiple spaces in user-supplied
    // expressions are accepted, while identifiers like `my_between` are
    // not. Literal-space matching here would falsely reject valid
    // multi-line expressions.
    let upper = part.to_ascii_uppercase();
    let bytes = upper.as_bytes();
    let between_pos = (0..bytes.len().saturating_sub(7)).find(|&i| {
        bytes[i..].starts_with(b"BETWEEN")
            && i > 0
            && (bytes[i - 1] as char).is_ascii_whitespace()
            && bytes
                .get(i + 7)
                .map(|c| (*c as char).is_ascii_whitespace())
                .unwrap_or(false)
    });
    if let Some(between_pos) = between_pos {
        return key_cond_between(part, between_pos, item, expr_attr_names, expr_attr_values);
    }

    key_cond_simple_comparison(part, item, expr_attr_names, expr_attr_values)
}

/// `begins_with(attr, :val)` — KeyCondition variant: supports only
/// S-typed attributes (mirrors AWS's behavior of returning false for
/// type mismatches). The filter-expression evaluator has its own
/// `eval_begins_with` because it operates on filter-grammar inputs.
pub(crate) fn key_cond_begins_with(
    rest: &str,
    item: &HashMap<String, AttributeValue>,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> bool {
    let Some(inner) = rest.strip_suffix(')') else {
        return false;
    };
    let mut split = inner.splitn(2, ',');
    let (Some(attr_ref), Some(val_ref)) = (split.next(), split.next()) else {
        return false;
    };
    let attr_name = resolve_attr_name(attr_ref.trim(), expr_attr_names);
    let expected = expr_attr_values.get(val_ref.trim());
    let actual = item.get(&attr_name);
    match (actual, expected) {
        (Some(a), Some(e)) => {
            let a_str = a.get("S").and_then(|v| v.as_str());
            let e_str = e.get("S").and_then(|v| v.as_str());
            matches!((a_str, e_str), (Some(a), Some(e)) if a.starts_with(e))
        }
        _ => false,
    }
}

/// `attr BETWEEN :lo AND :hi` — inclusive range comparison via the
/// shared `compare_attribute_values` ordering.
pub(crate) fn key_cond_between(
    part: &str,
    between_pos: usize,
    item: &HashMap<String, AttributeValue>,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> bool {
    let attr_part = part[..between_pos].trim();
    let attr_name = resolve_attr_name(attr_part, expr_attr_names);
    let range_part = &part[between_pos + 7..];
    let Some(and_pos) = range_part.to_ascii_uppercase().find(" AND ") else {
        return false;
    };
    let lo_ref = range_part[..and_pos].trim();
    let hi_ref = range_part[and_pos + 5..].trim();
    let lo = expr_attr_values.get(lo_ref);
    let hi = expr_attr_values.get(hi_ref);
    let actual = item.get(&attr_name);
    match (actual, lo, hi) {
        (Some(a), Some(l), Some(h)) => {
            compare_attribute_values(Some(a), Some(l)) != std::cmp::Ordering::Less
                && compare_attribute_values(Some(a), Some(h)) != std::cmp::Ordering::Greater
        }
        _ => false,
    }
}

/// `attr <op> :val` — six operators (`=`, `<>`, `<`, `>`, `<=`, `>=`).
/// Multi-character operators come first in the search list so that `<=`
/// is not mistakenly matched as `<`.
pub(crate) fn key_cond_simple_comparison(
    part: &str,
    item: &HashMap<String, AttributeValue>,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> bool {
    for op in &["<=", ">=", "<>", "=", "<", ">"] {
        let Some(pos) = part.find(op) else {
            continue;
        };
        let left = part[..pos].trim();
        let right = part[pos + op.len()..].trim();
        let actual_owned = resolve_path(left, item, expr_attr_names);
        let actual = actual_owned.as_ref();
        let expected = expr_attr_values.get(right);

        return match *op {
            "=" => actual == expected,
            "<>" => actual != expected,
            "<" => compare_attribute_values(actual, expected) == std::cmp::Ordering::Less,
            ">" => compare_attribute_values(actual, expected) == std::cmp::Ordering::Greater,
            "<=" => {
                let cmp = compare_attribute_values(actual, expected);
                cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal
            }
            ">=" => {
                let cmp = compare_attribute_values(actual, expected);
                cmp == std::cmp::Ordering::Greater || cmp == std::cmp::Ordering::Equal
            }
            _ => false,
        };
    }
    false
}

/// Returns the "size" of a DynamoDB attribute value per AWS docs:
/// - S → character count
/// - B → decoded byte count
/// - SS/NS/BS → element count
/// - L → element count
/// - M → element count
///
/// `size()` is not valid on N, BOOL, or NULL per AWS; returns None for those so
/// the enclosing comparison evaluates to false (matching AWS's behavior of
/// silently filtering type-mismatched rows in FilterExpression context).
pub(crate) fn attribute_size(val: &Value) -> Option<usize> {
    if let Some(s) = val.get("S").and_then(|v| v.as_str()) {
        return Some(s.len());
    }
    if let Some(b) = val.get("B").and_then(|v| v.as_str()) {
        // B is base64-encoded — return decoded byte count
        let decoded_len = base64::engine::general_purpose::STANDARD
            .decode(b)
            .map(|v| v.len())
            .unwrap_or(b.len());
        return Some(decoded_len);
    }
    if let Some(arr) = val.get("SS").and_then(|v| v.as_array()) {
        return Some(arr.len());
    }
    if let Some(arr) = val.get("NS").and_then(|v| v.as_array()) {
        return Some(arr.len());
    }
    if let Some(arr) = val.get("BS").and_then(|v| v.as_array()) {
        return Some(arr.len());
    }
    if let Some(arr) = val.get("L").and_then(|v| v.as_array()) {
        return Some(arr.len());
    }
    if let Some(obj) = val.get("M").and_then(|v| v.as_object()) {
        return Some(obj.len());
    }
    None
}

/// Evaluate a `size(path) op :val` comparison expression.
pub(crate) fn evaluate_size_comparison(
    part: &str,
    item: &HashMap<String, AttributeValue>,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> Option<bool> {
    // Find the closing paren of size(...)
    let open = part.find('(')?;
    let close = part[open..].find(')')? + open;
    let path = part[open + 1..close].trim();
    let remainder = part[close + 1..].trim();

    // Parse operator and value ref
    let (op, val_ref) = if let Some(rest) = remainder.strip_prefix("<=") {
        ("<=", rest.trim())
    } else if let Some(rest) = remainder.strip_prefix(">=") {
        (">=", rest.trim())
    } else if let Some(rest) = remainder.strip_prefix("<>") {
        ("<>", rest.trim())
    } else if let Some(rest) = remainder.strip_prefix('<') {
        ("<", rest.trim())
    } else if let Some(rest) = remainder.strip_prefix('>') {
        (">", rest.trim())
    } else if let Some(rest) = remainder.strip_prefix('=') {
        ("=", rest.trim())
    } else {
        return None;
    };

    let actual_owned = resolve_path(path, item, expr_attr_names)?;
    let size = attribute_size(&actual_owned)? as f64;

    let expected = extract_number(&expr_attr_values.get(val_ref).cloned())?;

    Some(match op {
        "=" => (size - expected).abs() < f64::EPSILON,
        "<>" => (size - expected).abs() >= f64::EPSILON,
        "<" => size < expected,
        ">" => size > expected,
        "<=" => size <= expected,
        ">=" => size >= expected,
        _ => false,
    })
}

pub(crate) fn evaluate_filter_expression(
    expr: &str,
    item: &HashMap<String, AttributeValue>,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> bool {
    let trimmed = expr.trim();

    // Split on OR first (lower precedence), respecting parentheses
    let or_parts = split_on_or(trimmed);
    if or_parts.len() > 1 {
        return or_parts.iter().any(|part| {
            evaluate_filter_expression(part.trim(), item, expr_attr_names, expr_attr_values)
        });
    }

    // Then split on AND (higher precedence), respecting parentheses
    let and_parts = split_on_and(trimmed);
    if and_parts.len() > 1 {
        return and_parts.iter().all(|part| {
            evaluate_filter_expression(part.trim(), item, expr_attr_names, expr_attr_values)
        });
    }

    // Strip outer parentheses if present
    let stripped = strip_outer_parens(trimmed);
    if stripped != trimmed {
        return evaluate_filter_expression(stripped, item, expr_attr_names, expr_attr_values);
    }

    // Handle NOT prefix (case-insensitive). Accept both `NOT (...)` and the
    // no-space `NOT(...)` that python_dynamodb_lock and hand-written expressions
    // emit — mirroring extract_function_arg's func( / func ( tolerance. Requiring
    // the char after `NOT` to be whitespace or `(` avoids misparsing an identifier
    // like `NOTanattr`.
    if let Some(rest) = trimmed
        .get(..3)
        .filter(|p| p.eq_ignore_ascii_case("NOT"))
        .map(|_| &trimmed[3..])
        .filter(|after| after.starts_with(|c: char| c.is_ascii_whitespace() || c == '('))
    {
        return !evaluate_filter_expression(rest, item, expr_attr_names, expr_attr_values);
    }

    evaluate_single_filter_condition(trimmed, item, expr_attr_names, expr_attr_values)
}
