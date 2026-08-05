//! dynamodb helpers `conditions` concerns (audit-2026-05-19).

use super::*;

pub(crate) fn evaluate_condition(
    condition: &str,
    existing: Option<&HashMap<String, AttributeValue>>,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> Result<(), AwsServiceError> {
    evaluate_condition_with_return(condition, existing, expr_attr_names, expr_attr_values, None)
}

/// Like [`evaluate_condition`], but when the check fails and
/// `return_values_on_failure == Some("ALL_OLD")`, attach the conflicting item
/// to the `ConditionalCheckFailedException` (AWS returns it under `Item` so
/// optimistic-locking clients can read current state without an extra GetItem).
pub(crate) fn evaluate_condition_with_return(
    condition: &str,
    existing: Option<&HashMap<String, AttributeValue>>,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
    return_values_on_failure: Option<&str>,
) -> Result<(), AwsServiceError> {
    // ConditionExpression and FilterExpression share the same DynamoDB grammar,
    // so we delegate to evaluate_filter_expression. An empty map models "item
    // doesn't exist" correctly: attribute_exists → false, attribute_not_exists
    // → true, comparisons against missing attributes → None vs Some(val).
    let empty = HashMap::new();
    let item = existing.unwrap_or(&empty);
    if evaluate_filter_expression(condition, item, expr_attr_names, expr_attr_values) {
        return Ok(());
    }
    let mut fields = Vec::new();
    if return_values_on_failure == Some("ALL_OLD") {
        if let Some(existing_item) = existing {
            if let Ok(s) = serde_json::to_string(&json!(existing_item)) {
                fields.push(("Item".to_string(), s));
            }
        }
    }
    Err(AwsServiceError::aws_error_with_fields(
        StatusCode::BAD_REQUEST,
        "ConditionalCheckFailedException",
        "The conditional request failed",
        fields,
    ))
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
/// keyword act as separators. Single- and double-quoted spans are skipped too,
/// so a separator (or paren) inside a PartiQL string literal (`'a, b'`) or a
/// quoted attribute name (`"my,attr"`) is treated as data, not a boundary.
/// When splitting on ``AND``, each top-level ``BETWEEN`` keyword consumes the
/// next top-level ``AND`` as its own inner separator (``x BETWEEN :lo AND :hi``)
/// rather than letting it split the expression.
pub(crate) fn split_on_top_level_keyword<'a>(expr: &'a str, keyword: &str) -> Vec<&'a str> {
    let bytes = expr.as_bytes();
    let len = bytes.len();
    let kw = keyword.as_bytes();
    let is_and = keyword.eq_ignore_ascii_case("AND");

    let mut parts: Vec<&str> = Vec::new();
    let mut start = 0usize;
    let mut depth: i32 = 0;
    let mut between_skip: u32 = 0;
    let mut in_squote = false;
    let mut in_dquote = false;
    let mut i = 0usize;

    while i < len {
        let ch = bytes[i];
        // Skip quoted spans wholesale: a comma/keyword/paren inside a string
        // literal or a quoted identifier is data, never a separator.
        if in_squote {
            if ch == b'\'' {
                in_squote = false;
            }
            i += 1;
            continue;
        }
        if in_dquote {
            if ch == b'"' {
                in_dquote = false;
            }
            i += 1;
            continue;
        }
        if ch == b'\'' {
            in_squote = true;
            i += 1;
            continue;
        }
        if ch == b'"' {
            in_dquote = true;
            i += 1;
            continue;
        }
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

/// `begins_with(attr, :val)` — KeyCondition variant: matches a String or
/// Binary prefix via the shared [`attribute_begins_with`] helper (returns
/// false for type mismatches, mirroring AWS). The filter-expression
/// evaluator has its own `eval_begins_with` entry point because it operates
/// on filter-grammar inputs, but both share the operand comparison.
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
        (Some(a), Some(e)) => attribute_begins_with(a, e),
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
            comparable_types(Some(a), Some(l))
                && comparable_types(Some(a), Some(h))
                && compare_attribute_values(Some(a), Some(l)) != std::cmp::Ordering::Less
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

        // A comparison whose left attribute path does not resolve is false for
        // EVERY operator in DynamoDB (use attribute_exists/attribute_not_exists
        // to test presence). Previously `<>`/`<`/`<=` against a missing
        // attribute wrongly matched (bug-audit 2026-06-26, 1.5): `attr <> :v`
        // gave `None != Some(v)` -> true and `attr < :v` gave Less -> true, so
        // "not in terminal state" filters and `version <> :v` write guards
        // silently passed.
        if actual.is_none() {
            return false;
        }

        return match *op {
            "=" => values_equal(actual, expected),
            "<>" => !values_equal(actual, expected),
            "<" => {
                comparable_types(actual, expected)
                    && compare_attribute_values(actual, expected) == std::cmp::Ordering::Less
            }
            ">" => {
                comparable_types(actual, expected)
                    && compare_attribute_values(actual, expected) == std::cmp::Ordering::Greater
            }
            "<=" => {
                comparable_types(actual, expected) && {
                    let cmp = compare_attribute_values(actual, expected);
                    cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal
                }
            }
            ">=" => {
                comparable_types(actual, expected) && {
                    let cmp = compare_attribute_values(actual, expected);
                    cmp == std::cmp::Ordering::Greater || cmp == std::cmp::Ordering::Equal
                }
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
    } else {
        let rest = remainder.strip_prefix('=')?;
        ("=", rest.trim())
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

/// Which legacy condition family is being translated. `KeyConditions`
/// (the `Query` key) accept only the comparison + range operators a key
/// condition allows; the filter families (`QueryFilter` / `ScanFilter`)
/// accept the broader operator set the FilterExpression evaluator supports.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum LegacyConditionRole {
    Key,
    Filter,
}

/// Translate a legacy `KeyConditions` / `QueryFilter` / `ScanFilter` map into
/// the equivalent expression string, hoisting attribute names into `#leg*`
/// placeholders and values into `:legv*` placeholders in the supplied
/// expr-attr maps so the result can be fed straight through the existing
/// `evaluate_key_condition` / `evaluate_filter_expression` machinery.
///
/// AWS deprecated these parameters in 2014 in favor of the expression API,
/// but real DynamoDB and every SDK still accept them — this is a parity
/// translation, not new evaluation logic. Entries are AND-ed together
/// (the only join AWS supports for the legacy form). Iteration is in sorted
/// attribute-name order so the synthesized expression is deterministic.
pub(crate) fn translate_legacy_conditions(
    conditions: &serde_json::Map<String, Value>,
    role: LegacyConditionRole,
    conditional_operator: &str,
    names: &mut HashMap<String, String>,
    values: &mut HashMap<String, Value>,
) -> Result<String, AwsServiceError> {
    // Role-specific placeholder tag so the key-condition and filter
    // translations in a single Query don't clobber each other's entries in
    // the shared expr-attr maps.
    let tag = match role {
        LegacyConditionRole::Key => "k",
        LegacyConditionRole::Filter => "f",
    };

    let mut entries: Vec<(&String, &Value)> = conditions.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));

    let mut fragments: Vec<String> = Vec::with_capacity(entries.len());
    for (idx, (attr, spec)) in entries.iter().enumerate() {
        let operator = spec
            .get("ComparisonOperator")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                legacy_validation_err(format!(
                    "ComparisonOperator is required for the condition on attribute {attr}"
                ))
            })?;
        let value_list: &[Value] = spec
            .get("AttributeValueList")
            .and_then(|v| v.as_array())
            .map(|a| a.as_slice())
            .unwrap_or(&[]);

        // Hoist the attribute name into a collision-safe placeholder.
        let name_ph = format!("#leg{tag}{idx}");
        names.insert(name_ph.clone(), (*attr).clone());

        // Hoist each operand value into its own placeholder.
        let mut value_phs: Vec<String> = Vec::with_capacity(value_list.len());
        for (vi, v) in value_list.iter().enumerate() {
            let ph = format!(":legv{tag}{idx}_{vi}");
            values.insert(ph.clone(), v.clone());
            value_phs.push(ph);
        }

        fragments.push(build_legacy_fragment(&name_ph, operator, &value_phs, role)?);
    }

    // KeyConditions are always implicitly AND-ed; ConditionalOperator only
    // applies to the filter forms (QueryFilter/ScanFilter). A single fragment
    // needs no joiner, so OR vs AND only matters with 2+ conditions.
    let joiner =
        if role == LegacyConditionRole::Filter && conditional_operator.eq_ignore_ascii_case("OR") {
            " OR "
        } else {
            " AND "
        };
    Ok(fragments.join(joiner))
}

/// Resolve the effective condition for a write op (Put/Update/Delete): prefer
/// `ConditionExpression`, otherwise translate the legacy `Expected` map,
/// injecting its placeholders into `names`/`values`. Supplying both forms is
/// rejected, matching real DynamoDB.
pub(crate) fn resolve_write_condition(
    body: &Value,
    names: &mut HashMap<String, String>,
    values: &mut HashMap<String, Value>,
) -> Result<Option<String>, AwsServiceError> {
    let expression = body["ConditionExpression"]
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let expected = body["Expected"].as_object().filter(|m| !m.is_empty());
    match (expression, expected) {
        (Some(_), Some(_)) => Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "Can not use both expression and non-expression parameters in the same request: \
             Non-expression parameters: {Expected} Expression parameters: {ConditionExpression}",
        )),
        (Some(expr), None) => Ok(Some(expr.to_string())),
        (None, Some(expected)) => {
            let op = body["ConditionalOperator"].as_str().unwrap_or("AND");
            Ok(Some(translate_legacy_expected(
                expected, op, names, values,
            )?))
        }
        (None, None) => Ok(None),
    }
}

/// Translate a legacy `Expected` map (pre-2014 conditional Put/Update/Delete)
/// into a ConditionExpression, hoisting attribute names/values into the shared
/// placeholder maps. `conditional_operator` is `"AND"` (default) or `"OR"`.
///
/// Each entry is either the newer `ComparisonOperator`/`AttributeValueList`
/// form or the legacy `Value`/`Exists` shorthand:
/// - `Exists: false` -> `attribute_not_exists`
/// - `Exists: true` (or omitted) with `Value` -> equality
/// - `Exists: true` without `Value` -> `attribute_exists`
pub(crate) fn translate_legacy_expected(
    expected: &serde_json::Map<String, Value>,
    conditional_operator: &str,
    names: &mut HashMap<String, String>,
    values: &mut HashMap<String, Value>,
) -> Result<String, AwsServiceError> {
    let mut entries: Vec<(&String, &Value)> = expected.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));

    let mut fragments: Vec<String> = Vec::with_capacity(entries.len());
    for (idx, (attr, spec)) in entries.iter().enumerate() {
        let name_ph = format!("#exp{idx}");
        names.insert(name_ph.clone(), (*attr).clone());

        // Newer ComparisonOperator form takes precedence when present.
        if spec.get("ComparisonOperator").is_some() {
            let operator = spec
                .get("ComparisonOperator")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let value_list: &[Value] = spec
                .get("AttributeValueList")
                .and_then(|v| v.as_array())
                .map(|a| a.as_slice())
                .unwrap_or(&[]);
            let mut value_phs: Vec<String> = Vec::with_capacity(value_list.len());
            for (vi, v) in value_list.iter().enumerate() {
                let ph = format!(":expv{idx}_{vi}");
                values.insert(ph.clone(), v.clone());
                value_phs.push(ph);
            }
            fragments.push(build_legacy_fragment(
                &name_ph,
                operator,
                &value_phs,
                LegacyConditionRole::Filter,
            )?);
            continue;
        }

        // Legacy Value / Exists shorthand.
        let exists = spec.get("Exists").and_then(|v| v.as_bool());
        let value = spec.get("Value");
        match (exists, value) {
            (Some(false), _) => fragments.push(format!("attribute_not_exists({name_ph})")),
            (_, Some(v)) => {
                let ph = format!(":expv{idx}");
                values.insert(ph.clone(), v.clone());
                fragments.push(format!("{name_ph} = {ph}"));
            }
            (Some(true), None) => fragments.push(format!("attribute_exists({name_ph})")),
            (None, None) => {
                return Err(legacy_validation_err(format!(
                    "One of Value or Exists must be provided for the Expected condition on attribute {attr}"
                )));
            }
        }
    }

    let joiner = if conditional_operator.eq_ignore_ascii_case("OR") {
        " OR "
    } else {
        " AND "
    };
    Ok(fragments.join(joiner))
}

fn legacy_validation_err(message: String) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", message)
}

/// Build the expression fragment for a single legacy condition entry.
fn build_legacy_fragment(
    name_ph: &str,
    operator: &str,
    values: &[String],
    role: LegacyConditionRole,
) -> Result<String, AwsServiceError> {
    // Operators a key condition may use; everything else is filter-only.
    let key_ok = matches!(
        operator,
        "EQ" | "LE" | "LT" | "GE" | "GT" | "BEGINS_WITH" | "BETWEEN"
    );
    if role == LegacyConditionRole::Key && !key_ok {
        return Err(legacy_validation_err(format!(
            "Unsupported operator on KeyConditions: {operator}"
        )));
    }

    let want = |n: usize| -> Result<(), AwsServiceError> {
        if values.len() == n {
            Ok(())
        } else {
            Err(legacy_validation_err(format!(
                "ComparisonOperator {operator} requires {n} value(s) in AttributeValueList, got {}",
                values.len()
            )))
        }
    };

    let frag = match operator {
        "EQ" => {
            want(1)?;
            format!("{name_ph} = {}", values[0])
        }
        "NE" => {
            want(1)?;
            format!("{name_ph} <> {}", values[0])
        }
        "LE" => {
            want(1)?;
            format!("{name_ph} <= {}", values[0])
        }
        "LT" => {
            want(1)?;
            format!("{name_ph} < {}", values[0])
        }
        "GE" => {
            want(1)?;
            format!("{name_ph} >= {}", values[0])
        }
        "GT" => {
            want(1)?;
            format!("{name_ph} > {}", values[0])
        }
        "BEGINS_WITH" => {
            want(1)?;
            format!("begins_with({name_ph}, {})", values[0])
        }
        "BETWEEN" => {
            want(2)?;
            format!("{name_ph} BETWEEN {} AND {}", values[0], values[1])
        }
        "CONTAINS" => {
            want(1)?;
            format!("contains({name_ph}, {})", values[0])
        }
        "NOT_CONTAINS" => {
            want(1)?;
            format!("NOT contains({name_ph}, {})", values[0])
        }
        "NOT_NULL" => {
            want(0)?;
            format!("attribute_exists({name_ph})")
        }
        "NULL" => {
            want(0)?;
            format!("attribute_not_exists({name_ph})")
        }
        "IN" => {
            if values.is_empty() {
                return Err(legacy_validation_err(
                    "ComparisonOperator IN requires at least one value in AttributeValueList"
                        .to_string(),
                ));
            }
            format!("{name_ph} IN ({})", values.join(", "))
        }
        other => {
            return Err(legacy_validation_err(format!(
                "Unsupported ComparisonOperator: {other}"
            )));
        }
    };
    Ok(frag)
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

#[cfg(test)]
mod legacy_translation_tests {
    use super::*;
    use serde_json::json;

    fn item(pairs: &[(&str, Value)]) -> HashMap<String, AttributeValue> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn conditions(v: Value) -> serde_json::Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    #[test]
    fn key_conditions_eq_and_begins_with() {
        let mut names = HashMap::new();
        let mut values = HashMap::new();
        let expr = translate_legacy_conditions(
            &conditions(json!({
                "pk": {"AttributeValueList": [{"S": "a"}], "ComparisonOperator": "EQ"},
                "sk": {"AttributeValueList": [{"S": "ord#"}], "ComparisonOperator": "BEGINS_WITH"},
            })),
            LegacyConditionRole::Key,
            "AND",
            &mut names,
            &mut values,
        )
        .unwrap();

        // Sorted attribute order: pk (#legk0) before sk (#legk1).
        assert_eq!(
            expr,
            "#legk0 = :legvk0_0 AND begins_with(#legk1, :legvk1_0)"
        );

        let matching = item(&[("pk", json!({"S": "a"})), ("sk", json!({"S": "ord#1"}))]);
        let wrong_pk = item(&[("pk", json!({"S": "b"})), ("sk", json!({"S": "ord#1"}))]);
        let wrong_sk = item(&[("pk", json!({"S": "a"})), ("sk", json!({"S": "x"}))]);
        assert!(evaluate_key_condition(&expr, &matching, &names, &values));
        assert!(!evaluate_key_condition(&expr, &wrong_pk, &names, &values));
        assert!(!evaluate_key_condition(&expr, &wrong_sk, &names, &values));
    }

    #[test]
    fn key_conditions_between_maps_correctly() {
        let mut names = HashMap::new();
        let mut values = HashMap::new();
        let expr = translate_legacy_conditions(
            &conditions(json!({
                "n": {
                    "AttributeValueList": [{"N": "10"}, {"N": "20"}],
                    "ComparisonOperator": "BETWEEN",
                },
            })),
            LegacyConditionRole::Key,
            "AND",
            &mut names,
            &mut values,
        )
        .unwrap();
        assert_eq!(expr, "#legk0 BETWEEN :legvk0_0 AND :legvk0_1");

        let inside = item(&[("n", json!({"N": "15"}))]);
        let below = item(&[("n", json!({"N": "5"}))]);
        let above = item(&[("n", json!({"N": "25"}))]);
        assert!(evaluate_key_condition(&expr, &inside, &names, &values));
        assert!(!evaluate_key_condition(&expr, &below, &names, &values));
        assert!(!evaluate_key_condition(&expr, &above, &names, &values));
    }

    #[test]
    fn filter_operators_ne_contains_not_null() {
        let mut names = HashMap::new();
        let mut values = HashMap::new();
        let expr = translate_legacy_conditions(
            &conditions(json!({
                "color": {"AttributeValueList": [{"S": "red"}], "ComparisonOperator": "NE"},
                "tags": {"AttributeValueList": [{"S": "vip"}], "ComparisonOperator": "CONTAINS"},
                "zzz": {"ComparisonOperator": "NOT_NULL"},
            })),
            LegacyConditionRole::Filter,
            "AND",
            &mut names,
            &mut values,
        )
        .unwrap();
        // Sorted: color (#legf0), tags (#legf1), zzz (#legf2).
        assert_eq!(
            expr,
            "#legf0 <> :legvf0_0 AND contains(#legf1, :legvf1_0) AND attribute_exists(#legf2)"
        );

        let hit = item(&[
            ("color", json!({"S": "blue"})),
            ("tags", json!({"SS": ["vip", "x"]})),
            ("zzz", json!({"S": "present"})),
        ]);
        let miss_color = item(&[
            ("color", json!({"S": "red"})),
            ("tags", json!({"SS": ["vip"]})),
            ("zzz", json!({"S": "present"})),
        ]);
        let miss_zzz = item(&[
            ("color", json!({"S": "blue"})),
            ("tags", json!({"SS": ["vip"]})),
        ]);
        assert!(evaluate_filter_expression(&expr, &hit, &names, &values));
        assert!(!evaluate_filter_expression(
            &expr,
            &miss_color,
            &names,
            &values
        ));
        assert!(!evaluate_filter_expression(
            &expr, &miss_zzz, &names, &values
        ));
    }

    #[test]
    fn unknown_operator_rejected() {
        let mut names = HashMap::new();
        let mut values = HashMap::new();
        let err = translate_legacy_conditions(
            &conditions(json!({
                "a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "WAT"},
            })),
            LegacyConditionRole::Filter,
            "AND",
            &mut names,
            &mut values,
        )
        .unwrap_err();
        assert!(format!("{err:?}").contains("Unsupported ComparisonOperator"));
    }

    #[test]
    fn wrong_value_count_rejected() {
        let mut names = HashMap::new();
        let mut values = HashMap::new();
        let err = translate_legacy_conditions(
            &conditions(json!({
                "n": {"AttributeValueList": [{"N": "10"}], "ComparisonOperator": "BETWEEN"},
            })),
            LegacyConditionRole::Key,
            "AND",
            &mut names,
            &mut values,
        )
        .unwrap_err();
        assert!(format!("{err:?}").contains("requires 2 value"));
    }

    #[test]
    fn key_role_rejects_filter_only_operator() {
        let mut names = HashMap::new();
        let mut values = HashMap::new();
        let err = translate_legacy_conditions(
            &conditions(json!({
                "a": {"AttributeValueList": [{"S": "x"}], "ComparisonOperator": "CONTAINS"},
            })),
            LegacyConditionRole::Key,
            "AND",
            &mut names,
            &mut values,
        )
        .unwrap_err();
        assert!(format!("{err:?}").contains("Unsupported operator on KeyConditions"));
    }
}
