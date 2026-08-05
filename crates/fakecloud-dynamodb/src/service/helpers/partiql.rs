//! dynamodb helpers `partiql` concerns (audit-2026-05-19).

use super::*;

/// Find the first byte offset of `needle` in `hay` that lies OUTSIDE any
/// single-quoted string literal.
///
/// PartiQL string literals are delimited by single quotes, so operator and
/// clause-keyword scanning must ignore anything inside them: a `<` in
/// `'https://x?a<b'`, a `WHERE` in `SET note = 'go WHERE you want'`, or an
/// ` IN ` in `name = 'go IN store'` are all data, not syntax. A doubled `''`
/// (PartiQL's escaped quote) has no characters between the two quotes, so the
/// brief toggle it produces can never straddle a real needle.
pub(crate) fn find_outside_quotes(hay: &str, needle: &str) -> Option<usize> {
    if needle.is_empty() {
        return None;
    }
    let bytes = hay.as_bytes();
    let nbytes = needle.as_bytes();
    let mut in_quote = false;
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            in_quote = !in_quote;
            i += 1;
            continue;
        }
        if !in_quote && i + nbytes.len() <= bytes.len() && &bytes[i..i + nbytes.len()] == nbytes {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Compare two DynamoDB numeric attribute strings with full precision.
///
/// DynamoDB numbers are arbitrary-precision decimals; parsing to `f64`
/// rounds past 2^53. This compares the decimal representations directly:
/// sign, then integer magnitude (by length, then lexically), then the
/// fractional part. Falls back to `Equal` only if both sides are
/// unparseable. Handles exponent notation and negative zero.
fn compare_number_strings(x: &str, y: &str) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    // A malformed Number string has no defined ordering; callers that care about
    // equality (values_equal) pre-check validity, so leaving this Equal keeps
    // range/order comparisons undefined rather than inventing a lexical order.
    let (xn, xd) = match normalize_decimal(x) {
        Some(v) => v,
        None => return Ordering::Equal,
    };
    let (yn, yd) = match normalize_decimal(y) {
        Some(v) => v,
        None => return Ordering::Equal,
    };
    // (is_negative_x, is_negative_y): a negative value is always less than a
    // non-negative one. Only decide by sign when the signs actually differ.
    match (xn, yn) {
        (true, false) => return Ordering::Less,
        (false, true) => return Ordering::Greater,
        _ => {}
    }
    let mag = compare_magnitude(&xd, &yd);
    if xn {
        mag.reverse()
    } else {
        mag
    }
}

/// Whether `s` is a well-formed DynamoDB Number literal (a decimal string that
/// `normalize_decimal` can parse). Used to reject malformed `{"N":...}` operands
/// before they are persisted (ADD on a new attribute, bug-hunt 2026-07-01).
pub(crate) fn is_valid_number(s: &str) -> bool {
    normalize_decimal(s).is_some()
}

/// Number of significant digits in a DynamoDB Number literal, or `None` if the
/// string is not a valid number. Leading zeros, trailing zeros, the sign, the
/// decimal point and the exponent are all insignificant — DynamoDB stores a
/// number as a coefficient plus an exponent, so `1E38` (a 39-character string)
/// is a single significant digit while `1234...` with 39 non-zero digits is 39.
/// AWS rejects a Number with more than 38 significant digits.
pub(crate) fn significant_digit_count(s: &str) -> Option<usize> {
    let (_, (int_norm, frac_norm)) = normalize_decimal(s)?;
    // `int_norm` already has leading zeros stripped and `frac_norm` trailing
    // zeros stripped; the remaining leading zeros (value < 1) and trailing
    // zeros (integer with a zeroed tail) collapse into the exponent.
    let coeff = format!("{int_norm}{frac_norm}");
    Some(coeff.trim_start_matches('0').trim_end_matches('0').len())
}

/// Canonical decimal form of a DynamoDB Number, so numerically-equal literals
/// (`"1"`, `"1.0"`, `"1e0"`) collapse to one key. Used to detect duplicate
/// members in a Number Set (`NS`), which AWS compares by numeric value rather
/// than string. Returns `None` for a malformed number.
pub(crate) fn canonical_number(s: &str) -> Option<String> {
    let (neg, (int_part, frac_part)) = normalize_decimal(s)?;
    let sign = if neg { "-" } else { "" };
    let int_str = if int_part.is_empty() { "0" } else { &int_part };
    if frac_part.is_empty() {
        Some(format!("{sign}{int_str}"))
    } else {
        Some(format!("{sign}{int_str}.{frac_part}"))
    }
}

/// Decompose a decimal string into `(is_negative, (int_digits, frac_digits))`
/// with insignificant zeros stripped so the magnitude compare is purely
/// lexical. Returns `None` for non-numeric input; negative zero normalizes
/// to positive.
#[allow(clippy::type_complexity)]
fn normalize_decimal(s: &str) -> Option<(bool, (String, String))> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (neg, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (mantissa, exp) = match rest.split_once(['e', 'E']) {
        Some((m, e)) => (m, e.parse::<i64>().ok()?),
        None => (rest, 0),
    };
    let (int_part, frac_part) = match mantissa.split_once('.') {
        Some((i, f)) => (i, f),
        None => (mantissa, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let mut digits = format!("{int_part}{frac_part}");
    let point = int_part.len() as i64 + exp;
    let (int_str, frac_str) = if point <= 0 {
        let pad = "0".repeat((-point) as usize);
        digits = format!("{pad}{digits}");
        (String::new(), digits)
    } else if (point as usize) >= digits.len() {
        let pad = "0".repeat(point as usize - digits.len());
        digits.push_str(&pad);
        (digits, String::new())
    } else {
        let (i, f) = digits.split_at(point as usize);
        (i.to_string(), f.to_string())
    };
    let int_norm = int_str.trim_start_matches('0').to_string();
    let frac_norm = frac_str.trim_end_matches('0').to_string();
    let is_zero = int_norm.is_empty() && frac_norm.is_empty();
    Some((neg && !is_zero, (int_norm, frac_norm)))
}

/// Lexically compare two normalized non-negative magnitudes
/// `(int_digits, frac_digits)`.
fn compare_magnitude(a: &(String, String), b: &(String, String)) -> std::cmp::Ordering {
    a.0.len()
        .cmp(&b.0.len())
        .then_with(|| a.0.cmp(&b.0))
        .then_with(|| a.1.cmp(&b.1))
}

/// Add (`is_add`) or subtract two DynamoDB Number strings with arbitrary
/// precision, returning the normalized result string.
///
/// DynamoDB numbers are 38-significant-digit decimals; parsing operands to
/// `f64` rounds past 2^53 and saturates an `as i64` cast past ~9.2e18, so a
/// `SET #c = #c + :inc` or `ADD #c :inc` on a large counter silently corrupts
/// the value. This works directly on the decimal digit strings instead.
/// Returns `None` only if either operand is not a valid number.
/// bug-audit 2026-06-28.
pub(crate) fn decimal_add_sub(a: &str, b: &str, is_add: bool) -> Option<String> {
    let (a_neg, (a_int, a_frac)) = normalize_decimal(a)?;
    let (mut b_neg, (b_int, b_frac)) = normalize_decimal(b)?;
    if !is_add {
        b_neg = !b_neg;
    }

    let (res_neg, (res_int, res_frac)) = if a_neg == b_neg {
        // Same sign: add magnitudes, keep the sign.
        (a_neg, add_magnitude(&a_int, &a_frac, &b_int, &b_frac))
    } else {
        // Opposite signs: subtract the smaller magnitude from the larger and
        // take the larger's sign.
        match compare_magnitude(
            &(a_int.clone(), a_frac.clone()),
            &(b_int.clone(), b_frac.clone()),
        ) {
            std::cmp::Ordering::Equal => (false, (String::new(), String::new())),
            std::cmp::Ordering::Greater => (a_neg, sub_magnitude(&a_int, &a_frac, &b_int, &b_frac)),
            std::cmp::Ordering::Less => (b_neg, sub_magnitude(&b_int, &b_frac, &a_int, &a_frac)),
        }
    };

    Some(format_decimal(res_neg, &res_int, &res_frac))
}

/// Right-pad a fractional digit string with zeros to `len`.
fn pad_frac(frac: &str, len: usize) -> String {
    let mut s = frac.to_string();
    s.extend(std::iter::repeat_n('0', len.saturating_sub(s.len())));
    s
}

/// Split a digit string into `(int, frac)` where `frac` is the last
/// `frac_len` digits (front-padded with zeros if the string is too short).
fn split_at_frac(digits: &str, frac_len: usize) -> (String, String) {
    if digits.len() <= frac_len {
        let padded = format!("{}{}", "0".repeat(frac_len - digits.len()), digits);
        (String::new(), padded)
    } else {
        let (i, f) = digits.split_at(digits.len() - frac_len);
        (i.to_string(), f.to_string())
    }
}

/// Add two non-negative magnitudes given as `(int_digits, frac_digits)`.
fn add_magnitude(ai: &str, af: &str, bi: &str, bf: &str) -> (String, String) {
    let flen = af.len().max(bf.len());
    let a_digits = format!("{ai}{}", pad_frac(af, flen));
    let b_digits = format!("{bi}{}", pad_frac(bf, flen));
    split_at_frac(&add_digit_strings(&a_digits, &b_digits), flen)
}

/// Subtract magnitude `(bi, bf)` from `(ai, af)`, assuming the first is
/// greater than or equal to the second.
fn sub_magnitude(ai: &str, af: &str, bi: &str, bf: &str) -> (String, String) {
    let flen = af.len().max(bf.len());
    let a_digits = format!("{ai}{}", pad_frac(af, flen));
    let b_digits = format!("{bi}{}", pad_frac(bf, flen));
    split_at_frac(&sub_digit_strings(&a_digits, &b_digits), flen)
}

/// Schoolbook addition of two non-negative integer digit strings.
fn add_digit_strings(a: &str, b: &str) -> String {
    let a: Vec<u8> = a.bytes().rev().map(|c| c - b'0').collect();
    let b: Vec<u8> = b.bytes().rev().map(|c| c - b'0').collect();
    let n = a.len().max(b.len());
    let mut out = Vec::with_capacity(n + 1);
    let mut carry = 0u8;
    for i in 0..n {
        let x = a.get(i).copied().unwrap_or(0) + b.get(i).copied().unwrap_or(0) + carry;
        out.push(b'0' + x % 10);
        carry = x / 10;
    }
    if carry > 0 {
        out.push(b'0' + carry);
    }
    out.reverse();
    String::from_utf8(out).unwrap_or_else(|_| "0".to_string())
}

/// Schoolbook subtraction `a - b` of two non-negative integer digit strings,
/// assuming `a >= b`.
fn sub_digit_strings(a: &str, b: &str) -> String {
    let a: Vec<i16> = a.bytes().rev().map(|c| (c - b'0') as i16).collect();
    let b: Vec<i16> = b.bytes().rev().map(|c| (c - b'0') as i16).collect();
    let mut out = Vec::with_capacity(a.len());
    let mut borrow = 0i16;
    for (i, &ad) in a.iter().enumerate() {
        let mut x = ad - borrow - b.get(i).copied().unwrap_or(0);
        if x < 0 {
            x += 10;
            borrow = 1;
        } else {
            borrow = 0;
        }
        out.push(b'0' + x as u8);
    }
    while out.len() > 1 && *out.last().unwrap() == b'0' {
        out.pop();
    }
    out.reverse();
    String::from_utf8(out).unwrap_or_else(|_| "0".to_string())
}

/// Render a signed `(int, frac)` magnitude as a DynamoDB Number string with
/// insignificant zeros stripped and no trailing `.0` for integral results.
fn format_decimal(neg: bool, int: &str, frac: &str) -> String {
    let int_t = int.trim_start_matches('0');
    let frac_t = frac.trim_end_matches('0');
    let is_zero = int_t.is_empty() && frac_t.is_empty();
    let sign = if neg && !is_zero { "-" } else { "" };
    let int_disp = if int_t.is_empty() { "0" } else { int_t };
    if frac_t.is_empty() {
        format!("{sign}{int_disp}")
    } else {
        format!("{sign}{int_disp}.{frac_t}")
    }
}

/// Two AttributeValues are comparable by the relational operators (`<`, `<=`,
/// `>`, `>=`, BETWEEN) only when they share a scalar type (S, N, or B).
/// DynamoDB does not match a comparison across mismatched types; without this
/// guard, `compare_attribute_values` falls back to `Equal` for a type mismatch,
/// which wrongly satisfies `<=`/`>=`/BETWEEN.
pub(crate) fn comparable_types(a: Option<&Value>, b: Option<&Value>) -> bool {
    match (
        a.and_then(attribute_type_and_value),
        b.and_then(attribute_type_and_value),
    ) {
        (Some((ta, _)), Some((tb, _))) => ta == tb && matches!(ta, "S" | "N" | "B"),
        _ => false,
    }
}

pub(crate) fn compare_attribute_values(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(a), Some(b)) => {
            let a_type = attribute_type_and_value(a);
            let b_type = attribute_type_and_value(b);
            match (a_type, b_type) {
                (Some(("S", a_val)), Some(("S", b_val))) => {
                    let a_str = a_val.as_str().unwrap_or("");
                    let b_str = b_val.as_str().unwrap_or("");
                    a_str.cmp(b_str)
                }
                (Some(("N", a_val)), Some(("N", b_val))) => {
                    // DynamoDB numbers are arbitrary-precision decimals (up
                    // to 38 significant digits). Parsing to f64 silently
                    // rounds past 2^53, so two distinct large integers
                    // compare Equal and sort wrong (bug-audit 2026-05-28,
                    // 1.11). Compare the decimal strings directly.
                    let a_str = a_val.as_str().unwrap_or("0");
                    let b_str = b_val.as_str().unwrap_or("0");
                    compare_number_strings(a_str, b_str)
                }
                (Some(("B", a_val)), Some(("B", b_val))) => {
                    let a_str = a_val.as_str().unwrap_or("");
                    let b_str = b_val.as_str().unwrap_or("");
                    a_str.cmp(b_str)
                }
                _ => std::cmp::Ordering::Equal,
            }
        }
    }
}

/// Equality for `=` / `<>` that is numeric-aware: two N values are equal when
/// they are the same decimal regardless of formatting (`3.10` == `3.1`), while
/// every other type falls back to exact JSON equality (so Maps/Lists/Bool/sets
/// keep strict equality). Plain `a == b` on the raw JSON wrongly treated
/// decimally-equal numbers as unequal (bug-audit 2026-06-26, 1.16).
pub(crate) fn values_equal(a: Option<&Value>, b: Option<&Value>) -> bool {
    match (a, b) {
        (Some(av), Some(bv)) => {
            if let (Some(("N", an)), Some(("N", bn))) =
                (attribute_type_and_value(av), attribute_type_and_value(bv))
            {
                let (an, bn) = (
                    an.as_str().unwrap_or_default(),
                    bn.as_str().unwrap_or_default(),
                );
                // Only canonicalize when BOTH are valid numbers. A malformed
                // operand ({"N":"abc"}) must not compare equal to a valid stored
                // key, so fall back to strict byte-equality there -- otherwise
                // DeleteItem could delete the wrong row (Cubic P1, 2026-07-01).
                if is_valid_number(an) && is_valid_number(bn) {
                    compare_number_strings(an, bn) == std::cmp::Ordering::Equal
                } else {
                    av == bv
                }
            } else {
                av == bv
            }
        }
        (None, None) => true,
        _ => false,
    }
}

/// Whether two Number-set (`NS`) members are the same member. AWS compares NS
/// members by numeric value, so `"1"` and `"1.0"` are one member: a set can
/// never hold both, `ADD` dedups by value, and `DELETE` removes by value. The
/// members are the bare decimal strings stored inside the `NS` array. Malformed
/// members fall back to exact string equality so a bad operand can never be
/// mistaken for a valid stored member. Mirrors the read side's `values_equal`.
pub(crate) fn ns_members_equal(a: &Value, b: &Value) -> bool {
    match (a.as_str(), b.as_str()) {
        (Some(x), Some(y)) if is_valid_number(x) && is_valid_number(y) => {
            compare_number_strings(x, y) == std::cmp::Ordering::Equal
        }
        _ => a == b,
    }
}

pub(crate) fn execute_partiql_in_state(
    state: &mut crate::state::DynamoDbState,
    statement: &str,
    parameters: &[Value],
) -> Result<PartiqlOutcome, AwsServiceError> {
    let trimmed = statement.trim();
    let upper = trimmed.to_ascii_uppercase();

    if upper.starts_with("SELECT") {
        let from_pos = find_outside_quotes(&upper, "FROM").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Invalid SELECT statement: missing FROM",
            )
        })?;
        let after_from = trimmed[from_pos + 4..].trim();
        let (table_name, rest) = parse_partiql_table_name(after_from);
        let table = get_table(&state.tables, &table_name)?;
        let rest_upper = rest.trim().to_ascii_uppercase();
        let items: Vec<Value> = if rest_upper.starts_with("WHERE") {
            let where_clause = rest.trim()[5..].trim();
            evaluate_partiql_where(table, where_clause, parameters)?
                .iter()
                .map(|item| json!(item))
                .collect()
        } else {
            table.items.iter().map(|item| json!(item)).collect()
        };
        Ok(PartiqlOutcome {
            response: json!({ "Items": items }),
            table_name: Some(table_name),
            event_name: None,
            keys: None,
            old_image: None,
            new_image: None,
        })
    } else if upper.starts_with("INSERT") {
        let into_pos = find_outside_quotes(&upper, "INTO").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Invalid INSERT statement: missing INTO",
            )
        })?;
        let after_into = trimmed[into_pos + 4..].trim();
        let (table_name, rest) = parse_partiql_table_name(after_into);
        let rest_upper = rest.trim().to_ascii_uppercase();
        let value_pos = find_outside_quotes(&rest_upper, "VALUE").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Invalid INSERT statement: missing VALUE",
            )
        })?;
        let value_str = rest.trim()[value_pos + 5..].trim();
        let item = parse_partiql_value_object(value_str, parameters)?;
        let table = get_table_mut(&mut state.tables, &table_name)?;
        validate_partiql_item_against_key_schema(table, &item)?;
        let key = extract_key(table, &item);
        if table.find_item_index(&key).is_some() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DuplicateItemException",
                "Duplicate primary key exists in table",
            ));
        }
        table.items.push(item.clone());
        table.recalculate_stats();
        Ok(PartiqlOutcome {
            response: json!({}),
            table_name: Some(table_name),
            event_name: Some("INSERT".to_string()),
            keys: Some(key),
            old_image: None,
            new_image: Some(item),
        })
    } else if upper.starts_with("UPDATE") {
        let after_update = trimmed[6..].trim();
        let (table_name, rest) = parse_partiql_table_name(after_update);
        let rest_upper = rest.trim().to_ascii_uppercase();
        let set_pos = find_outside_quotes(&rest_upper, "SET").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Invalid UPDATE statement: missing SET",
            )
        })?;
        // Strip a trailing `RETURNING ...` clause from the whole post-SET
        // segment first, so it is removed whether or not a WHERE is present
        // (a WHERE-less `UPDATE t SET x = ? RETURNING *` would otherwise leave
        // RETURNING inside the SET clause and corrupt the update expression).
        let (after_set, returns_item) =
            split_partiql_returning_clause(rest.trim()[set_pos + 3..].trim());
        let where_pos = find_outside_quotes(&after_set.to_ascii_uppercase(), "WHERE");
        let (set_clause, where_clause) = if let Some(wp) = where_pos {
            (&after_set[..wp], after_set[wp + 5..].trim())
        } else {
            (after_set, "")
        };
        let table = get_table_mut(&mut state.tables, &table_name)?;
        // Positional `?` parameters bind in textual order: the SET clause comes
        // before WHERE, so SET consumes parameters[0..set_count] and WHERE the
        // rest. Evaluate WHERE against the parameters that follow the SET ones.
        let set_param_count = count_params_in_str(set_clause);
        let matched_indices = if !where_clause.is_empty() {
            let where_params: &[Value] = if set_param_count <= parameters.len() {
                &parameters[set_param_count..]
            } else {
                &[]
            };
            find_partiql_where_indices(table, where_clause, where_params)?
        } else {
            (0..table.items.len()).collect()
        };
        let (update_expression, expression_attribute_values) =
            prepare_partiql_update_expression(set_clause, parameters);
        let mut last_key: Option<HashMap<String, AttributeValue>> = None;
        let mut last_old: Option<HashMap<String, AttributeValue>> = None;
        let mut last_new: Option<HashMap<String, AttributeValue>> = None;
        for idx in &matched_indices {
            last_old = Some(table.items[*idx].clone());
            apply_update_expression(
                &mut table.items[*idx],
                &update_expression,
                &HashMap::new(),
                &expression_attribute_values,
            )?;
            last_key = Some(extract_key(table, &table.items[*idx]));
            last_new = Some(table.items[*idx].clone());
        }
        table.recalculate_stats();
        Ok(PartiqlOutcome {
            response: if returns_item {
                last_new
                    .as_ref()
                    .map(|item| json!({ "Item": item }))
                    .unwrap_or_else(|| json!({}))
            } else {
                json!({})
            },
            table_name: Some(table_name),
            event_name: last_old.as_ref().map(|_| "MODIFY".to_string()),
            keys: last_key,
            old_image: last_old,
            new_image: last_new,
        })
    } else if upper.starts_with("DELETE") {
        let from_pos = find_outside_quotes(&upper, "FROM").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Invalid DELETE statement: missing FROM",
            )
        })?;
        let after_from = trimmed[from_pos + 4..].trim();
        let (table_name, rest) = parse_partiql_table_name(after_from);
        let rest_upper = rest.trim().to_ascii_uppercase();
        if !rest_upper.starts_with("WHERE") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "DELETE requires a WHERE clause",
            ));
        }
        let where_clause = rest.trim()[5..].trim();
        let table = get_table_mut(&mut state.tables, &table_name)?;
        let mut indices = find_partiql_where_indices(table, where_clause, parameters)?;
        indices.sort_unstable();
        indices.reverse();
        let mut last_old: Option<HashMap<String, AttributeValue>> = None;
        let mut last_key: Option<HashMap<String, AttributeValue>> = None;
        for idx in indices {
            let removed = table.items.remove(idx);
            last_key = Some(extract_key(table, &removed));
            last_old = Some(removed);
        }
        table.recalculate_stats();
        Ok(PartiqlOutcome {
            response: json!({}),
            table_name: Some(table_name),
            event_name: last_old.as_ref().map(|_| "REMOVE".to_string()),
            keys: last_key,
            old_image: last_old,
            new_image: None,
        })
    } else {
        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!("Unsupported PartiQL statement: {trimmed}"),
        ))
    }
}

fn split_partiql_returning_clause(where_clause: &str) -> (&str, bool) {
    // `to_ascii_uppercase` preserves byte length and never touches non-ASCII
    // bytes, so char boundaries stay aligned between `upper` and `where_clause`.
    // Iterate real char boundaries (not raw byte indices) so a non-ASCII byte
    // in the predicate cannot make a slice land mid-character and panic.
    let upper = where_clause.to_ascii_uppercase();
    let mut in_quote = false;
    for (index, ch) in upper.char_indices() {
        if ch == '\'' {
            in_quote = !in_quote;
        }
        if !in_quote
            && upper[index..].starts_with("RETURNING")
            && index > 0
            && upper[..index].ends_with(char::is_whitespace)
            && upper[index + 9..].starts_with(char::is_whitespace)
        {
            return (where_clause[..index].trim(), true);
        }
    }
    (where_clause, false)
}

fn prepare_partiql_update_expression(
    set_clause: &str,
    parameters: &[Value],
) -> (String, HashMap<String, Value>) {
    let mut expression = String::from("SET ");
    let mut parameter_index = 0;
    let mut in_quote = false;

    for character in set_clause.trim().chars() {
        if character == '\'' {
            in_quote = !in_quote;
            expression.push(character);
            continue;
        }

        if !in_quote && character == '?' {
            expression.push_str(&format!(":p{parameter_index}"));
            parameter_index += 1;
            continue;
        }

        expression.push(character);
    }

    let expression = split_repeated_set_clauses(&expression);
    let values = parameters
        .iter()
        .enumerate()
        .map(|(index, value)| (format!(":p{index}"), value.clone()))
        .collect();

    (expression, values)
}

fn split_repeated_set_clauses(expression: &str) -> String {
    // Walk char boundaries and copy `char`s (never `byte as char`, which
    // corrupts multibyte UTF-8) so an accented/CJK/emoji value or attribute
    // name in the SET clause survives intact and no slice panics mid-character.
    let mut result = String::new();
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut chars = expression.char_indices();

    while let Some((index, ch)) = chars.next() {
        match ch {
            '\'' if !in_double_quote => in_single_quote = !in_single_quote,
            '"' if !in_single_quote => in_double_quote = !in_double_quote,
            _ => {}
        }
        if !in_single_quote
            && !in_double_quote
            && expression[index..].starts_with("SET")
            && index > 0
            && expression[..index].ends_with(char::is_whitespace)
            && expression[index + 3..].starts_with(char::is_whitespace)
        {
            result.push(',');
            // "SET" is three ASCII bytes; `ch` consumed the 'S', skip 'E','T'.
            chars.next();
            chars.next();
            continue;
        }
        result.push(ch);
    }

    result
}

/// Parse a table name that may be quoted with double quotes.
/// Returns (table_name, rest_of_string).
pub(crate) fn parse_partiql_table_name(s: &str) -> (String, &str) {
    let s = s.trim();
    if let Some(stripped) = s.strip_prefix('"') {
        // Quoted name
        if let Some(end) = stripped.find('"') {
            let name = &stripped[..end];
            let rest = &stripped[end + 1..];
            (name.to_string(), rest)
        } else {
            let end = s.find(' ').unwrap_or(s.len());
            (s[..end].trim_matches('"').to_string(), &s[end..])
        }
    } else {
        let end = s.find(|c: char| c.is_whitespace()).unwrap_or(s.len());
        (s[..end].to_string(), &s[end..])
    }
}

/// Evaluate a simple WHERE clause: `col = 'value'` or `col = ?`
/// Returns matching items.
pub(crate) fn evaluate_partiql_where<'a>(
    table: &'a DynamoTable,
    where_clause: &str,
    parameters: &[Value],
) -> Result<Vec<&'a HashMap<String, AttributeValue>>, AwsServiceError> {
    let indices = find_partiql_where_indices(table, where_clause, parameters)?;
    Ok(indices.iter().map(|i| &table.items[*i]).collect())
}

pub(crate) fn find_partiql_where_indices(
    table: &DynamoTable,
    where_clause: &str,
    parameters: &[Value],
) -> Result<Vec<usize>, AwsServiceError> {
    // Try the full expression parser first — supports AND/OR/NOT and
    // parenthesized groups. If the clause doesn't parse cleanly we
    // fall back to the legacy AND-only path so older callers that
    // emit non-standard syntax keep matching zero rows instead of
    // 500-ing.
    let expr = parse_partiql_where_expr(where_clause, parameters);
    if let Some(expr) = expr {
        let mut indices = Vec::new();
        for (i, item) in table.items.iter().enumerate() {
            if evaluate_partiql_expr(&expr, item) {
                indices.push(i);
            }
        }
        return Ok(indices);
    }

    let conditions = split_partiql_and_clauses(where_clause);
    let parsed_conditions = parse_partiql_conditions(&conditions, parameters);
    // If any clause failed to parse, fall back to the structured parser
    // instead of running a `.all([])` -> match-all evaluation. AWS
    // rejects unparseable PartiQL with ValidationException; mirror that
    // rather than silently UPDATE/DELETE every row in the table.
    if parsed_conditions.len() != conditions.len() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "Statement contains unsupported predicate(s); refusing match-all fallback",
        ));
    }

    let mut indices = Vec::new();
    for (i, item) in table.items.iter().enumerate() {
        let all_match = parsed_conditions
            .iter()
            .all(|c| evaluate_partiql_cond(c, item));
        if all_match {
            indices.push(i);
        }
    }

    Ok(indices)
}

pub(crate) fn evaluate_partiql_expr(
    expr: &PartiqlExpr,
    item: &HashMap<String, AttributeValue>,
) -> bool {
    match expr {
        PartiqlExpr::Cond(c) => evaluate_partiql_cond(c, item),
        PartiqlExpr::And(l, r) => evaluate_partiql_expr(l, item) && evaluate_partiql_expr(r, item),
        PartiqlExpr::Or(l, r) => evaluate_partiql_expr(l, item) || evaluate_partiql_expr(r, item),
        PartiqlExpr::Not(e) => !evaluate_partiql_expr(e, item),
    }
}

fn tokenize_partiql_where(where_clause: &str) -> Vec<WhereTok<'_>> {
    let bytes = where_clause.as_bytes();
    let upper = where_clause.to_ascii_uppercase();
    let upper_bytes = upper.as_bytes();
    let mut toks: Vec<WhereTok<'_>> = Vec::new();
    let mut i = 0usize;
    let mut atom_start: Option<usize> = None;
    let mut paren_depth: i32 = 0;
    let mut in_quote = false;
    let mut in_atom_paren = 0i32; // tracks `(...)` inside an atom

    fn push_atom<'a>(toks: &mut Vec<WhereTok<'a>>, src: &'a str, start: usize, end: usize) {
        let slice = src[start..end].trim();
        if !slice.is_empty() {
            toks.push(WhereTok::Atom(&src[start..end]));
        }
    }

    while i < bytes.len() {
        let c = bytes[i] as char;
        if in_quote {
            if c == '\'' {
                in_quote = false;
            }
            i += 1;
            continue;
        }
        if c == '\'' {
            if atom_start.is_none() {
                atom_start = Some(i);
            }
            in_quote = true;
            i += 1;
            continue;
        }

        // Inside an atom, track parens so begins_with(name, 'a') keeps
        // its inner `(` tied to the atom.
        if let Some(start) = atom_start {
            if c == '(' {
                in_atom_paren += 1;
                i += 1;
                continue;
            }
            if c == ')' {
                if in_atom_paren > 0 {
                    in_atom_paren -= 1;
                    i += 1;
                    continue;
                }
                // Top-level `)` closes a group — flush the atom first.
                push_atom(&mut toks, where_clause, start, i);
                atom_start = None;
                toks.push(WhereTok::RParen);
                paren_depth -= 1;
                i += 1;
                continue;
            }
            // Look for keyword boundaries: AND / OR / NOT surrounded
            // by whitespace.
            if c.is_whitespace() && in_atom_paren == 0 {
                if let Some((kw, len)) = match_where_keyword(upper_bytes, i) {
                    push_atom(&mut toks, where_clause, start, i);
                    atom_start = None;
                    toks.push(kw);
                    i += len;
                    continue;
                }
            }
            i += 1;
            continue;
        }

        // Not currently building an atom.
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        if c == '(' {
            toks.push(WhereTok::LParen);
            paren_depth += 1;
            i += 1;
            continue;
        }
        if c == ')' {
            toks.push(WhereTok::RParen);
            paren_depth -= 1;
            i += 1;
            continue;
        }
        // Could be a leading NOT.
        if let Some((kw, len)) = match_where_keyword_at_start(upper_bytes, i) {
            toks.push(kw);
            i += len;
            continue;
        }
        atom_start = Some(i);
        i += 1;
    }

    if let Some(start) = atom_start {
        push_atom(&mut toks, where_clause, start, bytes.len());
    }

    if paren_depth != 0 || in_quote {
        return Vec::new();
    }
    toks
}

/// Match ` AND `, ` OR `, ` NOT ` starting at `i` (`i` is the leading
/// whitespace). Returns the token plus the consumed length so the
/// scanner can advance past the trailing whitespace too.
fn match_where_keyword(upper: &[u8], i: usize) -> Option<(WhereTok<'static>, usize)> {
    // Need: whitespace at i, then keyword, then whitespace or `(`.
    if i >= upper.len() || !(upper[i] as char).is_whitespace() {
        return None;
    }
    let after = i + 1;
    let try_kw = |kw: &[u8], tok: WhereTok<'static>| -> Option<(WhereTok<'static>, usize)> {
        if after + kw.len() > upper.len() {
            return None;
        }
        if &upper[after..after + kw.len()] != kw {
            return None;
        }
        let trail = after + kw.len();
        if trail >= upper.len() {
            return Some((tok, trail - i));
        }
        let next = upper[trail] as char;
        if next.is_whitespace() || next == '(' {
            Some((tok, trail - i))
        } else {
            None
        }
    };
    if let Some(r) = try_kw(b"AND", WhereTok::And) {
        return Some(r);
    }
    if let Some(r) = try_kw(b"OR", WhereTok::Or) {
        return Some(r);
    }
    if let Some(r) = try_kw(b"NOT", WhereTok::Not) {
        return Some(r);
    }
    None
}

/// Match a leading `NOT`/`AND`/`OR` (binary ops appear here when they
/// follow a `)` token without preceding whitespace). No leading
/// whitespace requirement — the caller has already skipped it.
fn match_where_keyword_at_start(upper: &[u8], i: usize) -> Option<(WhereTok<'static>, usize)> {
    let try_kw = |kw: &[u8], tok: WhereTok<'static>| -> Option<(WhereTok<'static>, usize)> {
        if i + kw.len() > upper.len() {
            return None;
        }
        if &upper[i..i + kw.len()] != kw {
            return None;
        }
        let trail = i + kw.len();
        if trail >= upper.len() {
            return Some((tok, kw.len()));
        }
        let next = upper[trail] as char;
        if next.is_whitespace() || next == '(' {
            Some((tok, kw.len()))
        } else {
            None
        }
    };
    if let Some(r) = try_kw(b"NOT", WhereTok::Not) {
        return Some(r);
    }
    if let Some(r) = try_kw(b"AND", WhereTok::And) {
        return Some(r);
    }
    if let Some(r) = try_kw(b"OR", WhereTok::Or) {
        return Some(r);
    }
    None
}

/// Parse a WHERE clause into [`PartiqlExpr`]. Returns `None` when the
/// clause has no logical operators OR fails to parse — callers fall
/// back to the legacy AND-only evaluator in that case.
pub(crate) fn parse_partiql_where_expr(
    where_clause: &str,
    parameters: &[Value],
) -> Option<PartiqlExpr> {
    let toks = tokenize_partiql_where(where_clause);
    if toks.is_empty() {
        return None;
    }
    let mut idx = 0usize;
    let mut param_idx = 0usize;
    let expr = parse_or(&toks, &mut idx, parameters, &mut param_idx)?;
    if idx != toks.len() {
        return None;
    }
    Some(expr)
}

fn parse_or(
    toks: &[WhereTok<'_>],
    i: &mut usize,
    params: &[Value],
    param_idx: &mut usize,
) -> Option<PartiqlExpr> {
    let mut left = parse_and(toks, i, params, param_idx)?;
    while matches!(toks.get(*i), Some(WhereTok::Or)) {
        *i += 1;
        let right = parse_and(toks, i, params, param_idx)?;
        left = PartiqlExpr::Or(Box::new(left), Box::new(right));
    }
    Some(left)
}

fn parse_and(
    toks: &[WhereTok<'_>],
    i: &mut usize,
    params: &[Value],
    param_idx: &mut usize,
) -> Option<PartiqlExpr> {
    let mut left = parse_not(toks, i, params, param_idx)?;
    while matches!(toks.get(*i), Some(WhereTok::And)) {
        *i += 1;
        let right = parse_not(toks, i, params, param_idx)?;
        left = PartiqlExpr::And(Box::new(left), Box::new(right));
    }
    Some(left)
}

fn parse_not(
    toks: &[WhereTok<'_>],
    i: &mut usize,
    params: &[Value],
    param_idx: &mut usize,
) -> Option<PartiqlExpr> {
    if matches!(toks.get(*i), Some(WhereTok::Not)) {
        *i += 1;
        let inner = parse_not(toks, i, params, param_idx)?;
        return Some(PartiqlExpr::Not(Box::new(inner)));
    }
    parse_primary(toks, i, params, param_idx)
}

fn parse_primary(
    toks: &[WhereTok<'_>],
    i: &mut usize,
    params: &[Value],
    param_idx: &mut usize,
) -> Option<PartiqlExpr> {
    match toks.get(*i)? {
        WhereTok::LParen => {
            *i += 1;
            let inner = parse_or(toks, i, params, param_idx)?;
            if !matches!(toks.get(*i), Some(WhereTok::RParen)) {
                return None;
            }
            *i += 1;
            Some(inner)
        }
        WhereTok::Atom(s) => {
            *i += 1;
            // Each atom may consume one or more `?` parameters; track
            // them globally so the order matches statement order.
            let cond = parse_one_partiql_condition(s.trim(), params, param_idx)?;
            Some(PartiqlExpr::Cond(cond))
        }
        _ => None,
    }
}

pub(crate) fn evaluate_partiql_cond(
    cond: &PartiqlCond,
    item: &HashMap<String, AttributeValue>,
) -> bool {
    match cond {
        PartiqlCond::Eq(a, v) => item.get(a) == Some(v),
        PartiqlCond::Ne(a, v) => item.get(a) != Some(v),
        PartiqlCond::Lt(a, v) => compare_attr(item.get(a), v).is_some_and(|c| c < 0),
        PartiqlCond::Le(a, v) => compare_attr(item.get(a), v).is_some_and(|c| c <= 0),
        PartiqlCond::Gt(a, v) => compare_attr(item.get(a), v).is_some_and(|c| c > 0),
        PartiqlCond::Ge(a, v) => compare_attr(item.get(a), v).is_some_and(|c| c >= 0),
        PartiqlCond::Between(a, lo, hi) => {
            let l = compare_attr(item.get(a), lo).is_some_and(|c| c >= 0);
            let r = compare_attr(item.get(a), hi).is_some_and(|c| c <= 0);
            l && r
        }
        PartiqlCond::In(a, vals) => match item.get(a) {
            Some(v) => vals.iter().any(|x| x == v),
            None => false,
        },
        PartiqlCond::Like(a, pattern) => {
            attr_string(item.get(a)).is_some_and(|s| match_like(&s, pattern))
        }
        PartiqlCond::BeginsWith(a, prefix) => attr_string(item.get(a))
            .zip(attr_string(Some(prefix)))
            .is_some_and(|(s, p)| s.starts_with(&p)),
        PartiqlCond::Contains(a, needle) => attr_string(item.get(a))
            .zip(attr_string(Some(needle)))
            .is_some_and(|(s, n)| s.contains(&n)),
        PartiqlCond::AttributeExists(a) => item.contains_key(a),
        PartiqlCond::AttributeNotExists(a) => !item.contains_key(a),
    }
}

/// Match a string against a PartiQL/SQL LIKE pattern. `%` matches any
/// run of characters (including empty), `_` matches exactly one
/// character. Both wildcards are anchored — `LIKE 'foo'` requires an
/// exact match, mirroring DDB PartiQL semantics.
pub(crate) fn match_like(s: &str, pattern: &str) -> bool {
    let s_chars: Vec<char> = s.chars().collect();
    let p_chars: Vec<char> = pattern.chars().collect();
    like_recurse(&s_chars, 0, &p_chars, 0)
}

fn like_recurse(s: &[char], si: usize, p: &[char], pi: usize) -> bool {
    if pi == p.len() {
        return si == s.len();
    }
    match p[pi] {
        '%' => {
            // Greedy backtracking: try matching 0..=remaining chars.
            for k in si..=s.len() {
                if like_recurse(s, k, p, pi + 1) {
                    return true;
                }
            }
            false
        }
        '_' => si < s.len() && like_recurse(s, si + 1, p, pi + 1),
        c => si < s.len() && s[si] == c && like_recurse(s, si + 1, p, pi + 1),
    }
}

/// Validate that every key-schema attribute is present in the item AND
/// that its AttributeValue carries the declared scalar type from
/// `attribute_definitions`. Real DDB rejects an INSERT or PutItem that
/// omits a key or supplies the wrong type with a `ValidationException`;
/// without the type check we'd silently accept e.g. `{'pk': 1}` for a
/// HASH key declared as `S`.
pub(crate) fn validate_partiql_item_against_key_schema(
    table: &DynamoTable,
    item: &HashMap<String, AttributeValue>,
) -> Result<(), AwsServiceError> {
    for key_attr in &table.key_schema {
        let Some(val) = item.get(&key_attr.attribute_name) else {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "One or more parameter values were invalid: Missing the key {} in the item",
                    key_attr.attribute_name
                ),
            ));
        };
        // Type check against AttributeDefinitions. AWS only allows
        // S/N/B for key attribute types.
        let declared = table
            .attribute_definitions
            .iter()
            .find(|d| d.attribute_name == key_attr.attribute_name)
            .map(|d| d.attribute_type.as_str());
        if let Some(expected) = declared {
            let obj = val.as_object();
            let actual_tag = obj.and_then(|o| o.keys().next().map(|k| k.as_str()));
            if actual_tag != Some(expected) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!(
                        "One or more parameter values were invalid: Type mismatch for key {} expected: {} actual: {}",
                        key_attr.attribute_name,
                        expected,
                        actual_tag.unwrap_or("?"),
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Three-way compare two AttributeValue payloads. Returns `Some(c)`
/// where the sign matches `lhs - rhs` (-1/0/+1), or `None` when the
/// comparison is undefined (mixed types, missing lhs, parse errors).
pub(crate) fn compare_attr(lhs: Option<&Value>, rhs: &Value) -> Option<i32> {
    let l = lhs?.as_object()?;
    let r = rhs.as_object()?;
    if let (Some(a), Some(b)) = (
        l.get("N").and_then(|v| v.as_str()),
        r.get("N").and_then(|v| v.as_str()),
    ) {
        // A malformed Number operand (`{"N":"abc"}`) has no defined ordering:
        // `compare_number_strings` falls back to `Equal` for it, which would
        // spuriously satisfy `<=`/`>=`/BETWEEN. Return `None` so the predicate
        // evaluates to false instead (bug-hunt 2026-07-01).
        if !is_valid_number(a) || !is_valid_number(b) {
            return None;
        }
        // Compare the decimal strings directly: parsing to f64 silently rounds
        // past 2^53, so two distinct large integers would compare Equal.
        return Some(match compare_number_strings(a, b) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        });
    }
    if let (Some(a), Some(b)) = (
        l.get("S").and_then(|v| v.as_str()),
        r.get("S").and_then(|v| v.as_str()),
    ) {
        return Some(match a.cmp(b) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        });
    }
    None
}

/// Pull the underlying string out of a PartiQL string-typed
/// AttributeValue (`{"S": "..."}`).
pub(crate) fn attr_string(v: Option<&Value>) -> Option<String> {
    v?.as_object()?.get("S")?.as_str().map(|s| s.to_string())
}

/// Parse a list of `<expr>` clauses into [`PartiqlCond`] entries.
/// Conditions that don't parse are silently dropped — the WHERE
/// clause yields zero matches in that case rather than 500-ing.
pub(crate) fn parse_partiql_conditions(
    conditions: &[&str],
    parameters: &[Value],
) -> Vec<PartiqlCond> {
    let mut param_idx = 0usize;
    let mut parsed = Vec::new();
    for cond in conditions {
        if let Some(c) = parse_one_partiql_condition(cond.trim(), parameters, &mut param_idx) {
            parsed.push(c);
        }
    }
    parsed
}

fn parse_one_partiql_condition(
    cond: &str,
    parameters: &[Value],
    param_idx: &mut usize,
) -> Option<PartiqlCond> {
    let upper = cond.to_ascii_uppercase();

    // Function-style: begins_with(attr, val), contains(attr, val),
    // attribute_exists(attr), attribute_not_exists(attr).
    if let Some(arg) = strip_func(cond, &upper, "ATTRIBUTE_EXISTS") {
        return Some(PartiqlCond::AttributeExists(strip_attr(arg)));
    }
    if let Some(arg) = strip_func(cond, &upper, "ATTRIBUTE_NOT_EXISTS") {
        return Some(PartiqlCond::AttributeNotExists(strip_attr(arg)));
    }
    if let Some(args) = strip_func(cond, &upper, "BEGINS_WITH") {
        let (attr, val) = split_two_args(args, parameters, param_idx)?;
        return Some(PartiqlCond::BeginsWith(attr, val));
    }
    if let Some(args) = strip_func(cond, &upper, "CONTAINS") {
        let (attr, val) = split_two_args(args, parameters, param_idx)?;
        return Some(PartiqlCond::Contains(attr, val));
    }

    // BETWEEN: `attr BETWEEN lo AND hi`. The split-on-AND step
    // already preserved the inner AND, so we see the full clause.
    // Keyword scans skip single-quoted spans so a literal like
    // `'x BETWEEN y'` on the RHS never fools the parser.
    if let Some(b) = find_outside_quotes(&upper, " BETWEEN ") {
        let attr = cond[..b].trim().trim_matches('"').to_string();
        let rest = cond[b + 9..].trim();
        let rest_upper = rest.to_ascii_uppercase();
        if let Some(a) = find_outside_quotes(&rest_upper, " AND ") {
            let lo = parse_partiql_literal(rest[..a].trim(), parameters, param_idx)?;
            let hi = parse_partiql_literal(rest[a + 5..].trim(), parameters, param_idx)?;
            return Some(PartiqlCond::Between(attr, lo, hi));
        }
    }

    // IN: `attr IN (a, b, c)`.
    if let Some(i) = find_outside_quotes(&upper, " IN ") {
        let attr = cond[..i].trim().trim_matches('"').to_string();
        let after = cond[i + 4..].trim();
        let inner = after
            .strip_prefix('(')
            .and_then(|s| s.strip_suffix(')'))?
            .trim();
        let mut vals = Vec::new();
        for raw in inner.split(',') {
            if let Some(v) = parse_partiql_literal(raw.trim(), parameters, param_idx) {
                vals.push(v);
            }
        }
        return Some(PartiqlCond::In(attr, vals));
    }

    // LIKE: `attr LIKE 'pattern'` (with `%` and `_` wildcards). The
    // pattern is always a string; we unwrap the {"S": ...} payload at
    // parse time so the evaluator can stay scalar-only.
    if let Some(l) = find_outside_quotes(&upper, " LIKE ") {
        let attr = cond[..l].trim().trim_matches('"').to_string();
        let rhs = cond[l + 6..].trim();
        let pattern_val = parse_partiql_literal(rhs, parameters, param_idx)?;
        let pattern = pattern_val
            .as_object()
            .and_then(|o| o.get("S"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())?;
        return Some(PartiqlCond::Like(attr, pattern));
    }

    // Operator-style. Order matters: longest operator first so `<=`
    // doesn't get parsed as `<`. The operator must sit OUTSIDE any
    // single-quoted literal, so a value like `'https://x?a<b'` is not
    // split at the `<` inside the string (bug-hunt 2026-07-01).
    for op in ["<>", "<=", ">=", "<", ">", "="] {
        if let Some(idx) = find_outside_quotes(cond, op) {
            let attr = cond[..idx].trim().trim_matches('"').to_string();
            let rhs = cond[idx + op.len()..].trim();

            // BETWEEN expressed as a chained `>=` AND `<=` is split on
            // " AND " upstream; the literal `BETWEEN x AND y` form is
            // handled below.
            let val = parse_partiql_literal(rhs, parameters, param_idx)?;
            return Some(match op {
                "=" => PartiqlCond::Eq(attr, val),
                "<>" => PartiqlCond::Ne(attr, val),
                "<=" => PartiqlCond::Le(attr, val),
                ">=" => PartiqlCond::Ge(attr, val),
                "<" => PartiqlCond::Lt(attr, val),
                ">" => PartiqlCond::Gt(attr, val),
                _ => return None,
            });
        }
    }

    None
}

fn strip_func<'a>(cond: &'a str, upper: &str, name: &str) -> Option<&'a str> {
    let prefix = format!("{name}(");
    if !upper.starts_with(&prefix) || !cond.ends_with(')') {
        return None;
    }
    Some(cond[prefix.len()..cond.len() - 1].trim())
}

fn strip_attr(s: &str) -> String {
    s.trim().trim_matches('"').to_string()
}

fn split_two_args(
    args: &str,
    parameters: &[Value],
    param_idx: &mut usize,
) -> Option<(String, Value)> {
    let (a, b) = args.split_once(',')?;
    let attr = strip_attr(a);
    let val = parse_partiql_literal(b.trim(), parameters, param_idx)?;
    Some((attr, val))
}

/// Split a PartiQL WHERE clause on case-insensitive ` AND ` boundaries.
/// Honors:
/// - `BETWEEN x AND y` — the inner AND must not split the clause
/// - `IN (a, b, c)` — internal commas/ANDs are inside parens, never matched
pub(crate) fn split_partiql_and_clauses(where_clause: &str) -> Vec<&str> {
    // ASCII-only uppercasing so `upper` stays byte-for-byte aligned with
    // `where_clause`; Unicode `to_uppercase()` can change byte length and make
    // the `upper.as_bytes()[i..i+5]` slices below run past the end (panic).
    let upper = where_clause.to_ascii_uppercase();
    if !upper.contains(" AND ") {
        return vec![where_clause.trim()];
    }
    let mut parts = Vec::new();
    let mut last = 0usize;
    let mut paren_depth: i32 = 0;
    let mut in_quote = false;
    let bytes = where_clause.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i] as char;
        match c {
            '\'' => in_quote = !in_quote,
            '(' if !in_quote => paren_depth += 1,
            ')' if !in_quote => paren_depth -= 1,
            _ => {}
        }
        if !in_quote
            && paren_depth == 0
            && i + 5 <= bytes.len()
            && upper.as_bytes()[i..i + 5] == *b" AND "
        {
            // Suppress this AND when it's the inner AND of a
            // BETWEEN: search backward for the most recent BETWEEN
            // since the previous split point and require that no
            // sibling AND has appeared between them.
            let segment = &upper[last..i];
            let in_between = segment
                .rfind(" BETWEEN ")
                .is_some_and(|b| segment[b + 9..].find(" AND ").is_none());
            if !in_between {
                parts.push(where_clause[last..i].trim());
                last = i + 5;
                i += 5;
                continue;
            }
        }
        i += 1;
    }
    parts.push(where_clause[last..].trim());
    parts
}

/// Parse a PartiQL literal value. Supports:
/// - 'string' -> {"S": "string"}
/// - 123 -> {"N": "123"}
/// - ? -> parameter from list
pub(crate) fn parse_partiql_literal(
    s: &str,
    parameters: &[Value],
    param_idx: &mut usize,
) -> Option<Value> {
    let s = s.trim();
    if s == "?" {
        let idx = *param_idx;
        *param_idx += 1;
        parameters.get(idx).cloned()
    } else if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
        let inner = &s[1..s.len() - 1];
        Some(json!({"S": inner}))
    } else if let Ok(n) = s.parse::<f64>() {
        // `f64::parse` accepts `inf`, `-inf`, and `NaN`, and overflows a
        // literal like `1e999` to infinity. DynamoDB Numbers are finite
        // decimals, so reject any non-finite literal rather than persisting a
        // bogus `{"N":"inf"}` / `{"N":"NaN"}` (bug-hunt 2026-07-01).
        if !n.is_finite() {
            return None;
        }
        // Preserve the exact decimal text for integer literals — going through
        // f64 silently rounds past 2^53, so a 17+ digit id would be stored
        // wrong. Fractional/scientific literals still normalize via f64.
        let is_integer = s
            .bytes()
            .all(|b| b.is_ascii_digit() || b == b'-' || b == b'+');
        let num_str = if is_integer {
            s.strip_prefix('+').unwrap_or(s).to_string()
        } else if n == n.trunc() {
            format!("{}", n as i64)
        } else {
            format!("{n}")
        };
        Some(json!({"N": num_str}))
    } else {
        None
    }
}

/// Parse a PartiQL VALUE object like `{'pk': 'val1', 'attr': 'val2'}` or with ? params.
pub(crate) fn parse_partiql_value_object(
    s: &str,
    parameters: &[Value],
) -> Result<HashMap<String, AttributeValue>, AwsServiceError> {
    let s = s.trim();
    let inner = s
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Invalid VALUE: expected object literal",
            )
        })?;

    let mut item = HashMap::new();
    let mut param_idx = 0usize;

    // Simple comma-separated key:value parsing
    for pair in split_partiql_pairs(inner) {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        if let Some((key_part, val_part)) = pair.split_once(':') {
            let key = key_part
                .trim()
                .trim_matches('\'')
                .trim_matches('"')
                .to_string();
            if let Some(val) = parse_partiql_literal(val_part.trim(), parameters, &mut param_idx) {
                item.insert(key, val);
            }
        }
    }

    Ok(item)
}

/// Split PartiQL object pairs on commas, respecting nested braces and quotes.
pub(crate) fn split_partiql_pairs(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut depth = 0;
    let mut in_quote = false;

    for (i, c) in s.char_indices() {
        match c {
            '\'' if !in_quote => in_quote = true,
            '\'' if in_quote => in_quote = false,
            '{' if !in_quote => depth += 1,
            '}' if !in_quote => depth -= 1,
            ',' if !in_quote && depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

#[cfg(test)]
mod number_compare_tests {
    use super::*;
    use std::cmp::Ordering;

    // bug-audit 2026-05-28, 1.11: integers past 2^53 must compare exactly,
    // not get rounded to the same f64.
    #[test]
    fn large_integers_compare_exactly() {
        assert_eq!(
            compare_number_strings("9007199254740993", "9007199254740992"),
            Ordering::Greater
        );
        assert_eq!(
            compare_number_strings("9007199254740992", "9007199254740993"),
            Ordering::Less
        );
        assert_eq!(
            compare_number_strings("12345678901234567890", "12345678901234567890"),
            Ordering::Equal
        );
    }

    #[test]
    fn signs_decimals_and_exponents() {
        assert_eq!(compare_number_strings("-5", "3"), Ordering::Less);
        assert_eq!(compare_number_strings("-5", "-3"), Ordering::Less);
        assert_eq!(compare_number_strings("3.14", "3.2"), Ordering::Less);
        assert_eq!(compare_number_strings("3.10", "3.1"), Ordering::Equal);
        assert_eq!(compare_number_strings("0", "-0"), Ordering::Equal);
        assert_eq!(compare_number_strings("10", "9"), Ordering::Greater);
        assert_eq!(compare_number_strings("1e3", "1000"), Ordering::Equal);
        assert_eq!(compare_number_strings("1e-2", "0.01"), Ordering::Equal);
    }

    // A negative value must always sort below a non-negative one regardless of
    // magnitude; the cross-sign arms must not be swapped.
    #[test]
    fn cross_sign_ordering_is_directional() {
        assert_eq!(compare_number_strings("-1", "1"), Ordering::Less);
        assert_eq!(compare_number_strings("1", "-1"), Ordering::Greater);
        // A small negative still loses to a large positive, and vice versa.
        assert_eq!(compare_number_strings("-100", "1"), Ordering::Less);
        assert_eq!(compare_number_strings("100", "-1"), Ordering::Greater);
        // Negative zero is normalized, so it ties zero rather than sorting low.
        assert_eq!(compare_number_strings("-0", "5"), Ordering::Less);
        assert_eq!(compare_number_strings("-0", "0"), Ordering::Equal);
        // Both negative: larger magnitude is the smaller value.
        assert_eq!(compare_number_strings("-100", "-1"), Ordering::Less);
        assert_eq!(compare_number_strings("-1", "-100"), Ordering::Greater);
    }

    // bug-audit 2026-06-28: SET/ADD arithmetic must be arbitrary precision.
    // f64 rounds past 2^53 and `as i64` saturates past ~9.2e18, corrupting
    // large counters; decimal_add_sub works on the digit strings instead.
    #[test]
    fn decimal_add_sub_big_integers_exact() {
        // Far beyond 2^53 (9007199254740992) and i64::MAX (9223372036854775807).
        assert_eq!(
            decimal_add_sub("9007199254740992", "1", true).unwrap(),
            "9007199254740993"
        );
        assert_eq!(
            decimal_add_sub("99999999999999999999999999999999999999", "1", true).unwrap(),
            "100000000000000000000000000000000000000"
        );
        assert_eq!(
            decimal_add_sub("9223372036854775807", "9223372036854775807", true).unwrap(),
            "18446744073709551614"
        );
    }

    #[test]
    fn decimal_add_sub_signs_and_fractions() {
        assert_eq!(decimal_add_sub("5", "3", false).unwrap(), "2");
        assert_eq!(decimal_add_sub("3", "5", false).unwrap(), "-2");
        assert_eq!(decimal_add_sub("-5", "3", true).unwrap(), "-2");
        assert_eq!(decimal_add_sub("5", "-3", true).unwrap(), "2");
        assert_eq!(decimal_add_sub("0.1", "0.2", true).unwrap(), "0.3");
        assert_eq!(decimal_add_sub("1.5", "2.5", true).unwrap(), "4");
        assert_eq!(decimal_add_sub("10", "10", false).unwrap(), "0");
        assert_eq!(decimal_add_sub("100.25", "0.75", true).unwrap(), "101");
        // Carry/borrow across the decimal point.
        assert_eq!(decimal_add_sub("1", "0.001", false).unwrap(), "0.999");
        // Non-numeric operand is rejected.
        assert!(decimal_add_sub("abc", "1", true).is_none());
    }
}

#[cfg(test)]
mod quote_aware_tests {
    use super::*;

    #[test]
    fn returning_clause_requires_keyword_boundaries() {
        assert_eq!(
            split_partiql_returning_clause("returning_status = ? RETURNING ALL NEW *"),
            ("returning_status = ?", true)
        );
        assert_eq!(
            split_partiql_returning_clause("note = 'RETURNING'"),
            ("note = 'RETURNING'", false)
        );
    }

    #[test]
    fn repeated_set_split_preserves_quoted_text() {
        assert_eq!(
            split_repeated_set_clauses("SET \"SET\" = 'ready SET now' SET state = ?"),
            "SET \"SET\" = 'ready SET now' , state = ?"
        );
    }

    // Non-ASCII values and attribute names must round-trip byte-for-byte: the
    // splitters walk char boundaries, so an accented / CJK / emoji character
    // neither corrupts the rebuilt expression nor panics a mid-char slice.
    #[test]
    fn repeated_set_split_preserves_non_ascii() {
        assert_eq!(
            split_repeated_set_clauses("SET \"café\" = 'José 名前 🚀' SET n = ?"),
            "SET \"café\" = 'José 名前 🚀' , n = ?"
        );
        // Unquoted non-ASCII attribute name must not panic.
        assert_eq!(split_repeated_set_clauses("SET café = ?"), "SET café = ?");
    }

    #[test]
    fn returning_clause_handles_non_ascii_predicate() {
        // A non-ASCII byte in the WHERE predicate (quoted or bare) must not
        // panic the byte-index scan.
        assert_eq!(
            split_partiql_returning_clause("\"café\" = ? RETURNING ALL NEW *"),
            ("\"café\" = ?", true)
        );
        assert_eq!(
            split_partiql_returning_clause("\"名前\" = ?"),
            ("\"名前\" = ?", false)
        );
    }

    // bug-hunt 2026-07-01: operator/keyword scans must skip single-quoted
    // literals so a `<` or `WHERE`/`IN` inside a string is treated as data.
    #[test]
    fn find_outside_quotes_skips_quoted_spans() {
        assert_eq!(find_outside_quotes("url = 'https://x?a<b'", "<"), None);
        assert_eq!(find_outside_quotes("url = 'a<b'", "="), Some(4));
        // The literal WHERE is skipped; the real (last) one is found.
        let s = "note = 'go WHERE you' WHERE id = 1";
        assert_eq!(find_outside_quotes(s, "WHERE"), s.rfind("WHERE"));
    }

    #[test]
    fn operator_scan_ignores_operator_inside_string_literal() {
        let mut idx = 0;
        let cond = parse_one_partiql_condition("url = 'https://x?a<b'", &[], &mut idx).unwrap();
        match cond {
            PartiqlCond::Eq(attr, val) => {
                assert_eq!(attr, "url");
                assert_eq!(val, json!({"S": "https://x?a<b"}));
            }
            other => panic!("expected Eq, got {other:?}"),
        }
    }

    #[test]
    fn parse_literal_rejects_non_finite_numbers() {
        let mut idx = 0;
        assert_eq!(parse_partiql_literal("1e999", &[], &mut idx), None);
        assert_eq!(parse_partiql_literal("inf", &[], &mut idx), None);
        assert_eq!(parse_partiql_literal("NaN", &[], &mut idx), None);
        assert_eq!(
            parse_partiql_literal("42", &[], &mut idx),
            Some(json!({"N": "42"}))
        );
    }

    // A malformed stored Number operand must not spuriously satisfy a
    // relational predicate (compare_attr returns None -> predicate false).
    #[test]
    fn compare_attr_rejects_malformed_number_operand() {
        assert_eq!(
            compare_attr(Some(&json!({"N": "abc"})), &json!({"N": "5"})),
            None
        );
        assert_eq!(
            compare_attr(Some(&json!({"N": "5"})), &json!({"N": "xyz"})),
            None
        );
        assert_eq!(
            compare_attr(Some(&json!({"N": "5"})), &json!({"N": "5"})),
            Some(0)
        );
    }
}

#[cfg(test)]
mod split_and_clause_tests {
    use super::*;

    // bug-audit 2026-06-27, T2.1: a length-shrinking non-ASCII char in the WHERE
    // clause must not panic. Unicode `to_uppercase()` could make the uppercased
    // buffer shorter than the original, so byte-index slices ran past its end.
    #[test]
    fn non_ascii_where_clause_does_not_panic() {
        // 'ﬀ' (U+FB00) is 3 bytes and uppercases to "FF" (2 bytes) under Unicode
        // folding — the exact shrink that triggered the original panic.
        let _ = split_partiql_and_clauses("ﬀ AND a = 1");
        let _ = split_partiql_and_clauses("name = 'café' AND id = 1");
        let _ = split_partiql_and_clauses("日本 BETWEEN 1 AND 10 AND x = 2");
        // Sanity: ASCII splitting still works.
        let parts = split_partiql_and_clauses("a = 1 AND b = 2");
        assert_eq!(parts.len(), 2);
    }
}
