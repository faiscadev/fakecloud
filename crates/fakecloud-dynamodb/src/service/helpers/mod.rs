//! Auto-extracted helper functions from mod.rs as part of carryover
//! service.rs split. PartiQL/condition/update-expression evaluators,
//! attribute-value plumbing, table description builders, etc.

#![allow(clippy::too_many_arguments)]

use std::collections::{BTreeMap, HashMap};

use base64::Engine;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::AwsServiceError;

use crate::state::*;

/// Parse an `OnDemandThroughput` block. Absent fields default to `-1`,
/// which is AWS's sentinel for "no cap" — and the value real AWS echoes
/// back on DescribeTable when the caller omitted either axis.
pub(super) fn parse_on_demand_throughput(val: &Value) -> Option<crate::state::OnDemandThroughput> {
    if !val.is_object() {
        return None;
    }
    Some(crate::state::OnDemandThroughput {
        max_read_request_units: val["MaxReadRequestUnits"].as_i64().unwrap_or(-1),
        max_write_request_units: val["MaxWriteRequestUnits"].as_i64().unwrap_or(-1),
    })
}

pub(super) fn parse_projection(val: &Value) -> Projection {
    Projection {
        projection_type: val["ProjectionType"].as_str().unwrap_or("ALL").to_string(),
        non_key_attributes: val["NonKeyAttributes"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default(),
    }
}

#[derive(Debug)]
pub(crate) enum PathSegment {
    Key(String),
    Index(usize),
}

/// Strip matching outer parentheses from an expression.
pub(crate) fn strip_outer_parens(expr: &str) -> &str {
    let trimmed = expr.trim();
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        return trimmed;
    }
    // Verify the outer parens actually match each other
    let inner = &trimmed[1..trimmed.len() - 1];
    let mut depth = 0;
    for ch in inner.bytes() {
        match ch {
            b'(' => depth += 1,
            b')' => {
                if depth == 0 {
                    return trimmed; // closing paren matches something inside, not the outer one
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    if depth == 0 {
        inner
    } else {
        trimmed
    }
}

pub(crate) fn evaluate_single_filter_condition(
    part: &str,
    item: &HashMap<String, AttributeValue>,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> bool {
    if let Some(inner) = extract_function_arg(part, "attribute_exists") {
        return resolve_path(inner, item, expr_attr_names).is_some();
    }

    if let Some(inner) = extract_function_arg(part, "attribute_not_exists") {
        return resolve_path(inner, item, expr_attr_names).is_none();
    }

    if let Some(rest) = part
        .strip_prefix("begins_with(")
        .or_else(|| part.strip_prefix("begins_with ("))
    {
        return eval_begins_with(rest, item, expr_attr_names, expr_attr_values);
    }

    if let Some(rest) = part
        .strip_prefix("contains(")
        .or_else(|| part.strip_prefix("contains ("))
    {
        return eval_contains(rest, item, expr_attr_names, expr_attr_values);
    }

    if part.starts_with("size(") || part.starts_with("size (") {
        if let Some(result) =
            evaluate_size_comparison(part, item, expr_attr_names, expr_attr_values)
        {
            return result;
        }
    }

    if let Some(rest) = part
        .strip_prefix("attribute_type(")
        .or_else(|| part.strip_prefix("attribute_type ("))
    {
        return eval_attribute_type(rest, item, expr_attr_names, expr_attr_values);
    }

    if let Some((attr_ref, value_refs)) = parse_in_expression(part) {
        // Resolve the left operand as a full document path so `a.b IN (...)`
        // and `a[0] IN (...)` work, not just a top-level attribute name
        // (bug-hunt 2026-07-01).
        let actual = resolve_path(attr_ref, item, expr_attr_names);
        return evaluate_in_match(actual.as_ref(), &value_refs, expr_attr_values);
    }

    evaluate_single_key_condition(part, item, expr_attr_names, expr_attr_values)
}

/// `begins_with(path, :val)` — matches a String or Binary prefix (via
/// [`attribute_begins_with`]). Returns false on any parse failure or type
/// mismatch (the same shape DynamoDB returns: a malformed predicate is
/// silently false rather than an error).
pub(crate) fn eval_begins_with(
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
    let actual = resolve_path(attr_ref.trim(), item, expr_attr_names);
    let expected = expr_attr_values.get(val_ref.trim());
    match (actual.as_ref(), expected) {
        (Some(a), Some(e)) => attribute_begins_with(a, e),
        _ => false,
    }
}

/// `begins_with` operand comparison shared by ConditionExpression and
/// KeyConditionExpression so both stay in sync (Cubic P3, 2026-07-01). Matches
/// on a String prefix, or a Binary prefix comparing the base64-DECODED bytes
/// (base64 pads in 3-byte groups, so a raw-string prefix would be wrong across
/// boundaries). Any other/malformed operand pairing is silently false, which is
/// the shape DynamoDB returns for a malformed predicate.
pub(crate) fn attribute_begins_with(actual: &Value, expected: &Value) -> bool {
    if let (Some(a), Some(e)) = (
        actual.get("S").and_then(|v| v.as_str()),
        expected.get("S").and_then(|v| v.as_str()),
    ) {
        return a.starts_with(e);
    }
    if let (Some(a), Some(e)) = (
        actual.get("B").and_then(|v| v.as_str()),
        expected.get("B").and_then(|v| v.as_str()),
    ) {
        use base64::Engine;
        let dec = |s: &str| base64::engine::general_purpose::STANDARD.decode(s).ok();
        return matches!((dec(a), dec(e)), (Some(a), Some(e)) if a.starts_with(&e));
    }
    false
}

/// `contains(path, :val)` — substring check on S, set membership on
/// SS/NS/BS, and element membership on L. Other type pairings return
/// false.
pub(crate) fn eval_contains(
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
    let actual = resolve_path(attr_ref.trim(), item, expr_attr_names);
    let expected = expr_attr_values.get(val_ref.trim());
    let (Some(a), Some(e)) = (actual.as_ref(), expected) else {
        return false;
    };

    if let (Some(a_s), Some(e_s)) = (
        a.get("S").and_then(|v| v.as_str()),
        e.get("S").and_then(|v| v.as_str()),
    ) {
        return a_s.contains(e_s);
    }
    if let Some(set) = a.get("SS").and_then(|v| v.as_array()) {
        if let Some(val) = e.get("S") {
            return set.contains(val);
        }
    }
    if let Some(set) = a.get("NS").and_then(|v| v.as_array()) {
        if let Some(val) = e.get("N") {
            // Number-set membership is canonical: "1" contains "1.0"
            // (bug-hunt 2026-07-01).
            return set
                .iter()
                .any(|s| values_equal(Some(&json!({ "N": s })), Some(&json!({ "N": val }))));
        }
    }
    if let Some(set) = a.get("BS").and_then(|v| v.as_array()) {
        if let Some(val) = e.get("B") {
            return set.contains(val);
        }
    }
    if let Some(list) = a.get("L").and_then(|v| v.as_array()) {
        // List membership compares elements canonically so numeric entries
        // match regardless of scale (bug-hunt 2026-07-01).
        return list.iter().any(|el| values_equal(Some(el), Some(e)));
    }
    false
}

/// `attribute_type(path, :type)` — checks whether the attribute at `path`
/// is stored under the wire type identified by `:type` (one of the
/// DynamoDB type letters S/N/B/BOOL/NULL/SS/NS/BS/L/M).
pub(crate) fn eval_attribute_type(
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
    let actual = resolve_path(attr_ref.trim(), item, expr_attr_names);
    let expected_type = expr_attr_values
        .get(val_ref.trim())
        .and_then(|v| v.get("S"))
        .and_then(|v| v.as_str());
    let (Some(val), Some(t)) = (actual.as_ref(), expected_type) else {
        return false;
    };
    match t {
        "S" => val.get("S").is_some(),
        "N" => val.get("N").is_some(),
        "B" => val.get("B").is_some(),
        "BOOL" => val.get("BOOL").is_some(),
        "NULL" => val.get("NULL").is_some(),
        "SS" => val.get("SS").is_some(),
        "NS" => val.get("NS").is_some(),
        "BS" => val.get("BS").is_some(),
        "L" => val.get("L").is_some(),
        "M" => val.get("M").is_some(),
        _ => false,
    }
}

/// Parse an `attr IN (:v1, :v2, ...)` expression. Mirrors the DynamoDB
/// ConditionExpression / FilterExpression grammar where IN takes a single
/// operand on the left and 1–100 comma-separated value refs inside parens
/// on the right. Case-insensitive; tolerates missing spaces after commas
/// (aws-sdk-go's `expression` builder emits ", " but hand-built expressions
/// often use `strings.Join(..., ",")`). Returns None for non-IN inputs so
/// callers can fall through to their other grammar branches.
pub(crate) fn parse_in_expression(expr: &str) -> Option<(&str, Vec<&str>)> {
    let upper = expr.to_ascii_uppercase();
    let in_pos = upper.find(" IN ")?;
    let attr_ref = expr[..in_pos].trim();
    if attr_ref.is_empty() {
        return None;
    }
    let rest = expr[in_pos + 4..].trim_start();
    let inner = rest.strip_prefix('(')?.strip_suffix(')')?;
    let values: Vec<&str> = inner
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    if values.is_empty() {
        return None;
    }
    Some((attr_ref, values))
}

/// Return true iff `actual` equals any of the `value_refs` resolved through
/// `expr_attr_values`. A missing attribute never matches (mirrors AWS, which
/// evaluates `IN` against undefined attributes as false).
pub(crate) fn evaluate_in_match(
    actual: Option<&AttributeValue>,
    value_refs: &[&str],
    expr_attr_values: &HashMap<String, Value>,
) -> bool {
    value_refs.iter().any(|v_ref| {
        let expected = expr_attr_values.get(*v_ref);
        // Canonical compare so `{"N":"1"} IN ({"N":"1.0"})` matches, mirroring
        // DynamoDB's numeric equality (bug-hunt 2026-07-01).
        matches!((actual, expected), (Some(a), Some(e)) if values_equal(Some(a), Some(e)))
    })
}

/// One of the four DynamoDB ``UpdateExpression`` action keywords.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UpdateAction {
    Set,
    Remove,
    Add,
    Delete,
}

impl UpdateAction {
    /// All four keywords as written on the wire — these double as the search
    /// terms for ``parse_update_clauses``.
    const KEYWORDS: &'static [(&'static str, UpdateAction)] = &[
        ("SET", UpdateAction::Set),
        ("REMOVE", UpdateAction::Remove),
        ("ADD", UpdateAction::Add),
        ("DELETE", UpdateAction::Delete),
    ];

    fn keyword(self) -> &'static str {
        match self {
            UpdateAction::Set => "SET",
            UpdateAction::Remove => "REMOVE",
            UpdateAction::Add => "ADD",
            UpdateAction::Delete => "DELETE",
        }
    }
}

pub(crate) fn apply_update_expression(
    item: &mut HashMap<String, AttributeValue>,
    expr: &str,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> Result<(), AwsServiceError> {
    let clauses = parse_update_clauses(expr);
    if clauses.is_empty() && !expr.trim().is_empty() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "Invalid UpdateExpression: Syntax error; token: \"<expression>\"",
        ));
    }
    for (action, assignments) in &clauses {
        match action {
            UpdateAction::Set => {
                for assignment in assignments {
                    apply_set_assignment(item, assignment, expr_attr_names, expr_attr_values)?;
                }
            }
            UpdateAction::Remove => {
                for attr_ref in assignments {
                    remove_path(item, attr_ref.trim(), expr_attr_names);
                }
            }
            UpdateAction::Add => {
                for assignment in assignments {
                    apply_add_assignment(item, assignment, expr_attr_names, expr_attr_values)?;
                }
            }
            UpdateAction::Delete => {
                for assignment in assignments {
                    apply_delete_assignment(item, assignment, expr_attr_names, expr_attr_values)?;
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn parse_update_clauses(expr: &str) -> Vec<(UpdateAction, Vec<String>)> {
    let mut clauses: Vec<(UpdateAction, Vec<String>)> = Vec::new();
    let upper = expr.to_ascii_uppercase();
    let mut positions: Vec<(usize, UpdateAction)> = Vec::new();

    // Identifiers in DynamoDB expressions can contain underscores
    // (`my_set_field`) and `#`/`:` placeholder prefixes; treating any
    // non-alphanumeric as a keyword boundary mis-parses those as
    // genuine SET/ADD/REMOVE/DELETE clauses. Require an ASCII whitespace
    // boundary on each side so `set foo = ...` is a keyword, but
    // `:set_arg`, `my_set_field`, `#set` are not.
    let is_boundary = |b: u8| b == b' ' || b == b'\t' || b == b'\n' || b == b'\r';
    for &(kw, action) in UpdateAction::KEYWORDS {
        let mut search_from = 0;
        while let Some(pos) = upper[search_from..].find(kw) {
            let abs_pos = search_from + pos;
            let before_ok = abs_pos == 0 || is_boundary(expr.as_bytes()[abs_pos - 1]);
            let after_pos = abs_pos + kw.len();
            let after_ok = after_pos >= expr.len() || is_boundary(expr.as_bytes()[after_pos]);
            if before_ok && after_ok {
                positions.push((abs_pos, action));
            }
            search_from = abs_pos + kw.len();
        }
    }

    positions.sort_by_key(|(pos, _)| *pos);

    for (i, &(pos, action)) in positions.iter().enumerate() {
        let start = pos + action.keyword().len();
        let end = if i + 1 < positions.len() {
            positions[i + 1].0
        } else {
            expr.len()
        };
        let content = expr[start..end].trim();
        // Use a paren-aware split so that function-call arguments such as
        // `list_append(#a, :b)` are kept as a single assignment rather than
        // being torn apart at the inner comma.
        let assignments: Vec<String> = split_on_top_level_keyword(content, ",")
            .into_iter()
            .map(|s| s.trim().to_string())
            .collect();
        clauses.push((action, assignments));
    }

    clauses
}

pub(crate) fn apply_set_assignment(
    item: &mut HashMap<String, AttributeValue>,
    assignment: &str,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> Result<(), AwsServiceError> {
    let Some((left, right)) = assignment.split_once('=') else {
        return Ok(());
    };

    let left_trimmed = left.trim();
    let right = right.trim();

    // One RHS evaluator used for every LHS shape so `SET a.b = a.b + :d`,
    // `SET a.b = list_append(a.b, :list)`, and `SET a.b = if_not_exists(a.b, :v)`
    // all work against nested paths, not just top-level attributes. The evaluator
    // returns Ok(None) when the RHS is a no-op (if_not_exists where the target
    // already has a value, or an unresolvable plain reference).
    let new_value = evaluate_set_rhs(right, item, expr_attr_names, expr_attr_values)?;

    if is_dotted_path(left_trimmed) {
        // A None value is a no-op (if_not_exists skip, or unresolvable plain
        // ref) — matches top-level SET's silent-skip behavior for the same
        // shapes. Structural errors (missing parent map, non-map intermediate)
        // surface from assign_nested_path itself.
        let Some(v) = new_value else {
            return Ok(());
        };
        return assign_nested_path(item, left_trimmed, expr_attr_names, v);
    }

    // Split off a trailing `[N]` list-index suffix so we can resolve the
    // attribute name ref on its own. Without this, `resolve_attr_name` sees
    // "#items[0]" as a whole and misses the `#items` → `items` mapping.
    let (attr_ref, list_index) = match parse_list_index_suffix(left_trimmed) {
        Some((name, idx)) => (name, Some(idx)),
        None => (left_trimmed, None),
    };
    let attr = resolve_attr_name(attr_ref, expr_attr_names);

    let Some(v) = new_value else {
        return Ok(());
    };
    match list_index {
        Some(idx) => assign_list_index(item, &attr, idx, v),
        None => {
            item.insert(attr, v);
            Ok(())
        }
    }
}

/// Evaluate the RHS of a `SET` assignment without writing it anywhere.
/// Returns `Ok(Some(value))` with the computed value, `Ok(None)` for
/// no-op cases (if_not_exists where the target already has a value, or
/// an unresolvable plain reference in dotted-path context), or
/// `Err(ValidationException)` for type-mismatched arithmetic.
pub(crate) fn evaluate_set_rhs(
    right: &str,
    item: &HashMap<String, AttributeValue>,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> Result<Option<Value>, AwsServiceError> {
    // Arithmetic is checked first so a top-level `+`/`-` is split before the
    // `if_not_exists(`/`list_append(` prefixes are tested. The canonical
    // atomic-counter idiom `if_not_exists(#c, :zero) + :inc` (and its mirror
    // `:inc + if_not_exists(#c, :zero)`) has `if_not_exists` as an *operand*
    // of `+`, not the whole RHS. `parse_arithmetic` only splits at a top-level
    // operator (depth 0), so a bare `if_not_exists(a, :b)` or `list_append(a, b)`
    // call — whose only `+`/`-` would be inside the parens — falls through.
    if let Some((arith_left, arith_right, is_add)) = parse_arithmetic(right) {
        return evaluate_arithmetic_rhs(
            arith_left,
            arith_right,
            is_add,
            item,
            expr_attr_names,
            expr_attr_values,
        );
    }

    if let Some(rest) = right
        .strip_prefix("if_not_exists(")
        .or_else(|| right.strip_prefix("if_not_exists ("))
    {
        return Ok(evaluate_if_not_exists_rhs(
            rest,
            item,
            expr_attr_names,
            expr_attr_values,
        ));
    }

    if let Some(rest) = right
        .strip_prefix("list_append(")
        .or_else(|| right.strip_prefix("list_append ("))
    {
        return Ok(evaluate_list_append_rhs(
            rest,
            item,
            expr_attr_names,
            expr_attr_values,
        ));
    }

    Ok(resolve_ref_or_path(
        right,
        item,
        expr_attr_names,
        expr_attr_values,
    ))
}

/// Evaluate a single arithmetic operand. Each side of a `+`/`-` may itself be
/// an `if_not_exists(path, :default)` call (the initialize-or-increment idiom),
/// a value reference, or a document path. Returns the resolved AttributeValue
/// or `None` when the operand resolves to nothing (e.g. a missing attribute
/// with no `if_not_exists` guard).
pub(crate) fn evaluate_arithmetic_operand(
    operand: &str,
    item: &HashMap<String, AttributeValue>,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> Option<Value> {
    let operand = operand.trim();
    if let Some(rest) = operand
        .strip_prefix("if_not_exists(")
        .or_else(|| operand.strip_prefix("if_not_exists ("))
    {
        // As an arithmetic operand, `if_not_exists(path, :default)` evaluates to
        // the *value of path* when it exists, otherwise the default — unlike the
        // top-level SET-RHS variant, where an existing path collapses to a no-op
        // (`None`). Returning `None` here would drop the operand and fail the
        // whole expression with a type error.
        let inner = rest.strip_suffix(')')?;
        let mut split = inner.splitn(2, ',');
        let (check, default) = (split.next()?, split.next()?);
        return resolve_ref_or_path(check.trim(), item, expr_attr_names, expr_attr_values).or_else(
            || resolve_ref_or_path(default.trim(), item, expr_attr_names, expr_attr_values),
        );
    }
    resolve_ref_or_path(operand, item, expr_attr_names, expr_attr_values)
}

/// `if_not_exists(path, :val)` — evaluates to nothing when `path` already
/// resolves to a value, and to the default ref otherwise. `path` may be a
/// top-level attribute, a placeholder, or a dotted path inside an M-typed
/// attribute.
pub(crate) fn evaluate_if_not_exists_rhs(
    rest: &str,
    item: &HashMap<String, AttributeValue>,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> Option<Value> {
    let inner = rest.strip_suffix(')')?;
    let mut split = inner.splitn(2, ',');
    let (check, default) = (split.next()?, split.next()?);
    if resolve_ref_or_path(check.trim(), item, expr_attr_names, expr_attr_values).is_some() {
        return None;
    }
    resolve_ref_or_path(default.trim(), item, expr_attr_names, expr_attr_values)
}

/// `list_append(a, b)` — concatenate the L arrays of two list operands.
/// Either operand may be missing or non-list, in which case it contributes
/// nothing. Both operands may be value refs (`:list`) or document paths
/// (top-level or dotted).
pub(crate) fn evaluate_list_append_rhs(
    rest: &str,
    item: &HashMap<String, AttributeValue>,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> Option<Value> {
    let inner = rest.strip_suffix(')')?;
    let mut split = inner.splitn(2, ',');
    let (a_ref, b_ref) = (split.next()?, split.next()?);
    let a_val = resolve_ref_or_path(a_ref.trim(), item, expr_attr_names, expr_attr_values);
    let b_val = resolve_ref_or_path(b_ref.trim(), item, expr_attr_names, expr_attr_values);

    let mut merged = Vec::new();
    for v in [&a_val, &b_val].iter().copied().flatten() {
        if let Value::Object(obj) = v {
            if let Some(Value::Array(arr)) = obj.get("L") {
                merged.extend(arr.clone());
            }
        }
    }
    Some(json!({ "L": merged }))
}

/// `<arith_left> +/- <arith_right>` — both operands must resolve to N values.
/// A missing operand is rejected with the same `ValidationException` AWS
/// returns (SET arithmetic does NOT zero-default a missing attribute — that's
/// what `if_not_exists` is for; the ADD action has its own zero-default path).
pub(crate) fn evaluate_arithmetic_rhs(
    arith_left: &str,
    arith_right: &str,
    is_add: bool,
    item: &HashMap<String, AttributeValue>,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> Result<Option<Value>, AwsServiceError> {
    let left_val = evaluate_arithmetic_operand(arith_left, item, expr_attr_names, expr_attr_values);
    let right_val =
        evaluate_arithmetic_operand(arith_right, item, expr_attr_names, expr_attr_values);

    let bad_operand = || {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "An operand in the update expression has an incorrect data type",
        )
    };

    let left_num = left_val
        .as_ref()
        .and_then(|v| v.get("N"))
        .and_then(|n| n.as_str())
        .ok_or_else(bad_operand)?;
    let right_num = right_val
        .as_ref()
        .and_then(|v| v.get("N"))
        .and_then(|n| n.as_str())
        .ok_or_else(bad_operand)?;

    // Arbitrary-precision decimal arithmetic — f64 silently rounds past 2^53
    // and an `as i64` cast saturates past ~9.2e18, corrupting large counters.
    let num_str = decimal_add_sub(left_num, right_num, is_add).ok_or_else(bad_operand)?;

    Ok(Some(json!({ "N": num_str })))
}

/// Parse a trailing `[N]` list-index suffix off the LHS of a SET assignment.
/// Returns the bare attribute reference and the index, or None when the LHS
/// is a plain attribute (or a path shape we don't yet support).
pub(crate) fn parse_list_index_suffix(path: &str) -> Option<(&str, usize)> {
    let path = path.trim();
    if !path.ends_with(']') {
        return None;
    }
    let open = path.rfind('[')?;
    // Require no further `.` / `[` / `]` inside the bracketed portion and no
    // further path segments after — we only handle the single-index case
    // `name[N]`, not nested shapes like `a.b[0].c`.
    let idx_str = &path[open + 1..path.len() - 1];
    let idx: usize = idx_str.parse().ok()?;
    let name = &path[..open];
    if name.is_empty() || name.contains('[') || name.contains(']') || name.contains('.') {
        return None;
    }
    Some((name, idx))
}

/// Assign a value to a specific index of a `L`-typed attribute. If `idx` is
/// within the current list, replaces that slot; if it's at the end, appends.
/// AWS rejects writes beyond `len`, so we return a `ValidationException` for
/// out-of-range indices and non-list attributes.
pub(crate) fn assign_list_index(
    item: &mut HashMap<String, AttributeValue>,
    attr: &str,
    idx: usize,
    value: Value,
) -> Result<(), AwsServiceError> {
    let Some(existing) = item.get_mut(attr) else {
        return Err(invalid_document_path());
    };
    let Some(list) = existing.get_mut("L").and_then(|l| l.as_array_mut()) else {
        return Err(invalid_document_path());
    };
    if idx < list.len() {
        list[idx] = value;
    } else if idx == list.len() {
        list.push(value);
    } else {
        return Err(invalid_document_path());
    }
    Ok(())
}

pub(crate) fn invalid_document_path() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationException",
        "The document path provided in the update expression is invalid for update",
    )
}

/// Resolve a SET-RHS operand that may be either a value placeholder
/// (``:foo``) or a document path (top-level attribute, ``#name``, or a
/// dotted path like ``profile.email`` / ``#web.#count``).
pub(crate) fn resolve_ref_or_path(
    reference: &str,
    item: &HashMap<String, AttributeValue>,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> Option<Value> {
    let reference = reference.trim();
    if reference.starts_with(':') {
        return expr_attr_values.get(reference).cloned();
    }
    resolve_path(reference, item, expr_attr_names)
}

/// True if `path` targets a nested key inside an M-typed attribute. Bracketed
/// list indices (`a[0]`, `a.b[0]`) are not supported by the nested-SET writer.
pub(crate) fn is_dotted_path(path: &str) -> bool {
    path.contains('.') && !path.contains('[')
}

pub(crate) struct TableDescriptionInput<'a> {
    pub arn: &'a str,
    pub table_id: &'a str,
    pub key_schema: &'a [KeySchemaElement],
    pub attribute_definitions: &'a [AttributeDefinition],
    pub provisioned_throughput: &'a ProvisionedThroughput,
    pub gsi: &'a [GlobalSecondaryIndex],
    pub lsi: &'a [LocalSecondaryIndex],
    pub billing_mode: &'a str,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub item_count: i64,
    pub size_bytes: i64,
    pub status: &'a str,
    pub deletion_protection_enabled: bool,
    pub on_demand_throughput: Option<&'a crate::state::OnDemandThroughput>,
}

/// In-place PartiQL executor used by every PartiQL entry point
/// (ExecuteStatement, BatchExecuteStatement, ExecuteTransaction).
/// The caller holds the write lock for the batch — ExecuteTransaction
/// keeps it across the entire all-or-nothing apply phase, single-shot
/// callers acquire it for one statement. Returns the response body
/// Value plus the touched table name and (for write ops) the keys +
/// before/after images so the caller can emit stream + kinesis events
/// after the lock is released — mirroring the per-write hooks in
/// items.rs and the TransactWriteItems path.
pub(crate) struct PartiqlOutcome {
    pub response: Value,
    pub table_name: Option<String>,
    pub event_name: Option<String>, // INSERT, MODIFY, REMOVE
    pub keys: Option<HashMap<String, AttributeValue>>,
    pub old_image: Option<HashMap<String, AttributeValue>>,
    pub new_image: Option<HashMap<String, AttributeValue>>,
}

/// AST for a parsed PartiQL WHERE clause. Leaf conditions reuse
/// [`PartiqlCond`]; the tree adds AND/OR/NOT/parens composition added
/// in L4 so callers can express anything the DDB FilterExpression
/// language can.
#[derive(Debug, Clone)]
pub(crate) enum PartiqlExpr {
    Cond(PartiqlCond),
    And(Box<PartiqlExpr>, Box<PartiqlExpr>),
    Or(Box<PartiqlExpr>, Box<PartiqlExpr>),
    Not(Box<PartiqlExpr>),
}

/// Tokens produced by [`tokenize_partiql_where`]. We keep the original
/// source slice for `Atom` so the existing condition parser can be
/// reused without a second tokenizer pass.
#[derive(Debug, Clone)]
enum WhereTok<'a> {
    LParen,
    RParen,
    And,
    Or,
    Not,
    Atom(&'a str),
}

/// A parsed PartiQL WHERE clause condition. Equality remains the
/// hot path; comparison/range/membership/function ops were added in
/// L4 so PartiQL filters can express anything DDB's expression
/// language can.
#[derive(Debug, Clone)]
pub(crate) enum PartiqlCond {
    Eq(String, Value),
    Ne(String, Value),
    Lt(String, Value),
    Le(String, Value),
    Gt(String, Value),
    Ge(String, Value),
    Between(String, Value, Value),
    In(String, Vec<Value>),
    Like(String, String),
    BeginsWith(String, Value),
    Contains(String, Value),
    AttributeExists(String),
    AttributeNotExists(String),
}

/// Count positional `?` parameters in a PartiQL clause, ignoring any `?` that
/// appears inside a single-quoted string literal (e.g. `SET note = 'done?'`).
/// The UPDATE executor uses this count to slice parameters between the SET and
/// WHERE clauses, so a `?` counted inside a literal would shift every WHERE
/// binding by one and silently no-op the update (bug-hunt 2026-07-01).
pub(crate) fn count_params_in_str(s: &str) -> usize {
    let mut in_quote = false;
    let mut count = 0;
    for c in s.chars() {
        match c {
            '\'' => in_quote = !in_quote,
            '?' if !in_quote => count += 1,
            _ => {}
        }
    }
    count
}

mod conditions;
mod keys;
mod metrics;
pub(crate) mod partiql;
mod paths;
mod request;
pub(crate) mod schemas;
mod table_descriptions;
mod table_lookup;
mod updates;
pub(crate) use conditions::*;
pub(crate) use keys::*;
pub(crate) use metrics::*;
pub(crate) use partiql::*;
pub(crate) use paths::*;
pub(crate) use request::*;
pub(crate) use schemas::*;
pub(crate) use table_descriptions::*;
pub(crate) use table_lookup::*;
pub(crate) use updates::*;

#[cfg(test)]
mod count_params_tests {
    use super::*;

    // bug-hunt 2026-07-01: a `?` inside a single-quoted literal must NOT be
    // counted, else the SET/WHERE parameter split shifts and no-ops the update.
    #[test]
    fn count_params_ignores_quoted_question_marks() {
        assert_eq!(count_params_in_str("SET note = 'done?' WHERE id = ?"), 1);
        assert_eq!(count_params_in_str("a = ? AND b = ?"), 2);
        assert_eq!(count_params_in_str("note = 'a?b?c'"), 0);
    }
}

#[cfg(test)]
mod set_rhs_tests {
    use super::*;
    use serde_json::json;

    fn names() -> HashMap<String, String> {
        HashMap::from([("#c".to_string(), "count".to_string())])
    }
    fn values() -> HashMap<String, Value> {
        HashMap::from([
            (":zero".to_string(), json!({"N": "0"})),
            (":inc".to_string(), json!({"N": "1"})),
        ])
    }

    // bug-audit 2026-06-28: the canonical initialize-or-increment counter idiom
    // `SET #c = if_not_exists(#c, :zero) + :inc` was a silent no-op (the whole
    // RHS was handed to the if_not_exists evaluator, which bailed). Both operand
    // orderings must compute `(if_not_exists(c, 0)) + 1`.
    #[test]
    fn atomic_counter_from_absent_both_orderings() {
        let item: HashMap<String, AttributeValue> = HashMap::new();
        let r1 = evaluate_set_rhs(
            "if_not_exists(#c, :zero) + :inc",
            &item,
            &names(),
            &values(),
        )
        .unwrap();
        assert_eq!(r1, Some(json!({"N": "1"})));
        let r2 = evaluate_set_rhs(
            ":inc + if_not_exists(#c, :zero)",
            &item,
            &names(),
            &values(),
        )
        .unwrap();
        assert_eq!(r2, Some(json!({"N": "1"})));
    }

    #[test]
    fn atomic_counter_from_existing_both_orderings() {
        let item: HashMap<String, AttributeValue> =
            HashMap::from([("count".to_string(), json!({"N": "41"}))]);
        let r1 = evaluate_set_rhs(
            "if_not_exists(#c, :zero) + :inc",
            &item,
            &names(),
            &values(),
        )
        .unwrap();
        assert_eq!(r1, Some(json!({"N": "42"})));
        let r2 = evaluate_set_rhs(
            ":inc + if_not_exists(#c, :zero)",
            &item,
            &names(),
            &values(),
        )
        .unwrap();
        assert_eq!(r2, Some(json!({"N": "42"})));
    }

    // A bare if_not_exists / list_append (no top-level operator) still routes to
    // its own evaluator rather than being misparsed as arithmetic.
    #[test]
    fn bare_if_not_exists_still_works() {
        let item: HashMap<String, AttributeValue> = HashMap::new();
        let r = evaluate_set_rhs("if_not_exists(#c, :zero)", &item, &names(), &values()).unwrap();
        assert_eq!(r, Some(json!({"N": "0"})));
    }
}

// bug-hunt 2026-07-01 LOW findings 79/80: FilterExpression `IN` / `contains`
// used raw serde_json equality (so `1` != `1.0`) and `IN` ignored nested
// document paths. Both must mirror DynamoDB's canonical numeric equality and
// full path resolution.
#[cfg(test)]
mod filter_in_contains_tests {
    use super::*;
    use serde_json::json;

    fn no_names() -> HashMap<String, String> {
        HashMap::new()
    }

    // `a.b IN (:v)` must resolve the nested path, not look up a top-level `a.b`.
    #[test]
    fn in_matches_nested_path() {
        let item: HashMap<String, AttributeValue> =
            HashMap::from([("a".to_string(), json!({"M": {"b": {"N": "5"}}}))]);
        let values = HashMap::from([(":v".to_string(), json!({"N": "5"}))]);
        assert!(evaluate_single_filter_condition(
            "a.b IN (:v)",
            &item,
            &no_names(),
            &values
        ));
    }

    // `IN` numeric comparison is canonical: 5 matches "5.0".
    #[test]
    fn in_matches_canonical_number() {
        let item: HashMap<String, AttributeValue> =
            HashMap::from([("n".to_string(), json!({"N": "5"}))]);
        let values = HashMap::from([(":v".to_string(), json!({"N": "5.0"}))]);
        assert!(evaluate_single_filter_condition(
            "n IN (:v)",
            &item,
            &no_names(),
            &values
        ));
    }

    // A missing attribute never matches IN.
    #[test]
    fn in_missing_attribute_no_match() {
        let item: HashMap<String, AttributeValue> = HashMap::new();
        let values = HashMap::from([(":v".to_string(), json!({"N": "5"}))]);
        assert!(!evaluate_single_filter_condition(
            "n IN (:v)",
            &item,
            &no_names(),
            &values
        ));
    }

    // contains() over a number set is canonical: NS {"1"} contains "1.0".
    #[test]
    fn contains_number_set_canonical() {
        let item: HashMap<String, AttributeValue> =
            HashMap::from([("s".to_string(), json!({"NS": ["1", "2"]}))]);
        let values = HashMap::from([(":v".to_string(), json!({"N": "1.0"}))]);
        assert!(evaluate_single_filter_condition(
            "contains(s, :v)",
            &item,
            &no_names(),
            &values
        ));
    }

    // contains() over a list is canonical for numeric elements.
    #[test]
    fn contains_list_canonical_number() {
        let item: HashMap<String, AttributeValue> =
            HashMap::from([("l".to_string(), json!({"L": [{"N": "3"}, {"S": "x"}]}))]);
        let values = HashMap::from([(":v".to_string(), json!({"N": "3.00"}))]);
        assert!(evaluate_single_filter_condition(
            "contains(l, :v)",
            &item,
            &no_names(),
            &values
        ));
    }
}
