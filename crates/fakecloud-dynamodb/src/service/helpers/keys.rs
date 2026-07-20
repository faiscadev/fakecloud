//! dynamodb helpers `keys` concerns (audit-2026-05-19).

use super::*;

/// Numeric-aware primary-key equality: two keys are equal when their hash (and
/// range, if any) attributes are the same DynamoDB value, so `{"N":"1"}` equals
/// `{"N":"1.0"}`. Used to detect duplicate keys in BatchGetItem.
pub(crate) fn keys_equal(
    table: &DynamoTable,
    a: &HashMap<String, AttributeValue>,
    b: &HashMap<String, AttributeValue>,
) -> bool {
    use super::partiql::values_equal;
    let hash_key = table.hash_key_name();
    // values_equal(None, None) is true, so require the hash key to actually be
    // present on both keys -- otherwise two keys both missing it would compare
    // equal (matches find_item_index's guard; Cubic P2, 2026-07-01).
    if !(values_equal(a.get(hash_key), b.get(hash_key))
        && a.get(hash_key).is_some()
        && b.get(hash_key).is_some())
    {
        return false;
    }
    match table.range_key_name() {
        Some(rk) => values_equal(a.get(rk), b.get(rk)),
        None => true,
    }
}

pub(crate) fn extract_key(
    table: &DynamoTable,
    item: &HashMap<String, AttributeValue>,
) -> HashMap<String, AttributeValue> {
    let mut key = HashMap::new();
    let hash_key = table.hash_key_name();
    if let Some(v) = item.get(hash_key) {
        key.insert(hash_key.to_string(), v.clone());
    }
    if let Some(range_key) = table.range_key_name() {
        if let Some(v) = item.get(range_key) {
            key.insert(range_key.to_string(), v.clone());
        }
    }
    key
}

/// Parse a JSON object into a key map (used for ExclusiveStartKey).
pub(crate) fn parse_key_map(value: &Value) -> Option<HashMap<String, AttributeValue>> {
    let obj = value.as_object()?;
    if obj.is_empty() {
        return None;
    }
    Some(obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
}

/// Check whether an item's key attributes match the given key map.
pub(crate) fn item_matches_key(
    item: &HashMap<String, AttributeValue>,
    key: &HashMap<String, AttributeValue>,
    hash_key_name: &str,
    range_key_name: Option<&str>,
) -> bool {
    let hash_match = match (item.get(hash_key_name), key.get(hash_key_name)) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    };
    if !hash_match {
        return false;
    }
    match range_key_name {
        Some(rk) => match (item.get(rk), key.get(rk)) {
            (Some(a), Some(b)) => a == b,
            (None, None) => true,
            _ => false,
        },
        None => true,
    }
}

/// Extract the primary key from an item given explicit key attribute names.
pub(crate) fn extract_key_for_schema(
    item: &HashMap<String, AttributeValue>,
    hash_key_name: &str,
    range_key_name: Option<&str>,
) -> HashMap<String, AttributeValue> {
    let mut key = HashMap::new();
    if let Some(v) = item.get(hash_key_name) {
        key.insert(hash_key_name.to_string(), v.clone());
    }
    if let Some(rk) = range_key_name {
        if let Some(v) = item.get(rk) {
            key.insert(rk.to_string(), v.clone());
        }
    }
    key
}

/// Recursively validate the AttributeValues in an item written by PutItem.
/// Real DynamoDB rejects a numeric attribute whose `N` value (or an `NS`
/// member) is not a valid decimal — e.g. `{"N":"abc"}` — with a
/// ValidationException, even for non-key attributes. Without this the bogus
/// value persists and later corrupts numeric comparisons / arithmetic
/// (bug-hunt 2026-07-01).
pub(crate) fn validate_item_attribute_values(
    item: &HashMap<String, AttributeValue>,
) -> Result<(), AwsServiceError> {
    for v in item.values() {
        validate_attribute_value(v)?;
    }
    Ok(())
}

/// Reject an empty DynamoDB set (`SS`/`NS`/`BS`). AWS returns a
/// `ValidationException` — storing an empty set corrupts later
/// `ADD`/`DELETE`/`size()` semantics.
fn validate_set_not_empty(kind: &str, is_empty: bool) -> Result<(), AwsServiceError> {
    if is_empty {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!("One or more parameter values were invalid: An {kind} set may not be empty"),
        ));
    }
    Ok(())
}

/// Build the `ValidationException` AWS returns when a set contains duplicate
/// members. The collection is echoed back the way AWS renders it.
fn duplicate_set(members: &[&str]) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationException",
        format!(
            "One or more parameter values were invalid: Input collection [{}] contains duplicates",
            members.join(", ")
        ),
    )
}

pub(crate) fn validate_attribute_value(v: &Value) -> Result<(), AwsServiceError> {
    let Some((tag, val)) = v.as_object().and_then(|o| o.iter().next()) else {
        return Ok(());
    };
    let bad_number = |n: &str| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!("The parameter cannot be converted to a numeric value: {n}"),
        )
    };
    // DynamoDB Numbers are decimals with at most 38 significant digits; a longer
    // coefficient is rejected with ValidationException.
    const MAX_SIGNIFICANT_DIGITS: usize = 38;
    let too_many_digits = || {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "Attempting to store more than 38 significant digits in a Number",
        )
    };
    match tag.as_str() {
        "N" => {
            let s = val.as_str().unwrap_or_default();
            match significant_digit_count(s) {
                None => return Err(bad_number(s)),
                Some(digits) if digits > MAX_SIGNIFICANT_DIGITS => return Err(too_many_digits()),
                Some(_) => {}
            }
        }
        "SS" => {
            let members: Vec<&str> = val
                .as_array()
                .into_iter()
                .flatten()
                .map(|el| el.as_str().unwrap_or_default())
                .collect();
            validate_set_not_empty("string", members.is_empty())?;
            let mut seen = std::collections::HashSet::new();
            for m in &members {
                if !seen.insert(*m) {
                    return Err(duplicate_set(&members));
                }
            }
        }
        "NS" => {
            let members: Vec<&str> = val
                .as_array()
                .into_iter()
                .flatten()
                .map(|el| el.as_str().unwrap_or_default())
                .collect();
            validate_set_not_empty("number", members.is_empty())?;
            let mut seen = std::collections::HashSet::new();
            for s in &members {
                match significant_digit_count(s) {
                    None => return Err(bad_number(s)),
                    Some(digits) if digits > MAX_SIGNIFICANT_DIGITS => {
                        return Err(too_many_digits())
                    }
                    Some(_) => {}
                }
                // Number-set members are deduped by numeric value, so `"1"` and
                // `"1.0"` collide. `canonical_number` never returns `None` here
                // because `is_valid_number` already passed.
                let canon = canonical_number(s).unwrap_or_else(|| (*s).to_string());
                if !seen.insert(canon) {
                    return Err(duplicate_set(&members));
                }
            }
        }
        "BS" => {
            use base64::Engine;
            let members: Vec<&str> = val
                .as_array()
                .into_iter()
                .flatten()
                .map(|el| el.as_str().unwrap_or_default())
                .collect();
            validate_set_not_empty("binary", members.is_empty())?;
            // Dedup by decoded bytes so distinct base64 encodings of the same
            // value still collide; fall back to the raw string when a member
            // is not valid base64 (left for the codec layer to reject).
            let mut seen = std::collections::HashSet::new();
            for m in &members {
                let key = base64::engine::general_purpose::STANDARD
                    .decode(m)
                    .unwrap_or_else(|_| m.as_bytes().to_vec());
                if !seen.insert(key) {
                    return Err(duplicate_set(&members));
                }
            }
        }
        "L" => {
            for el in val.as_array().into_iter().flatten() {
                validate_attribute_value(el)?;
            }
        }
        "M" => {
            if let Some(m) = val.as_object() {
                for el in m.values() {
                    validate_attribute_value(el)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn validate_key_in_item(
    table: &DynamoTable,
    item: &HashMap<String, AttributeValue>,
) -> Result<(), AwsServiceError> {
    let hash_key = table.hash_key_name();
    if !item.contains_key(hash_key) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!("Missing the key {hash_key} in the item"),
        ));
    }
    if let Some(range_key) = table.range_key_name() {
        if !item.contains_key(range_key) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("Missing the key {range_key} in the item"),
            ));
        }
    }
    check_key_type(table, item, hash_key)?;
    if let Some(range_key) = table.range_key_name() {
        check_key_type(table, item, range_key)?;
    }
    Ok(())
}

pub(crate) fn validate_key_attributes_in_key(
    table: &DynamoTable,
    key: &HashMap<String, AttributeValue>,
) -> Result<(), AwsServiceError> {
    let hash_key = table.hash_key_name();
    if !key.contains_key(hash_key) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!("Missing the key {hash_key} in the item"),
        ));
    }
    // Composite-key tables require BOTH hash and range in the Key map;
    // omitting the range key would otherwise let GetItem / DeleteItem
    // succeed with an under-specified key.
    if let Some(range_key) = table.range_key_name() {
        if !key.contains_key(range_key) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("Missing the key {range_key} in the item"),
            ));
        }
    }
    check_key_type(table, key, hash_key)?;
    if let Some(range_key) = table.range_key_name() {
        check_key_type(table, key, range_key)?;
    }
    Ok(())
}

/// Verify a key attribute present in `attrs` carries the scalar type declared
/// in the table's AttributeDefinitions. AWS rejects a wrong-typed key with
/// ValidationException; without this a `pk: S` table silently stored
/// `{"pk":{"N":"1"}}`, after which a correctly-typed GetItem couldn't find the
/// row -- the data appeared to vanish (bug-audit 2026-06-20, 1.13). The
/// PartiQL path already enforced this; the classic item API didn't.
fn check_key_type(
    table: &DynamoTable,
    attrs: &HashMap<String, AttributeValue>,
    name: &str,
) -> Result<(), AwsServiceError> {
    let Some(val) = attrs.get(name) else {
        return Ok(());
    };
    let Some(expected) = table
        .attribute_definitions
        .iter()
        .find(|d| d.attribute_name == name)
        .map(|d| d.attribute_type.as_str())
    else {
        return Ok(());
    };
    let actual = val
        .as_object()
        .and_then(|o| o.keys().next().map(|k| k.as_str()));
    if actual != Some(expected) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!(
                "One or more parameter values were invalid: Type mismatch for key {name} expected: {expected} actual: {}",
                actual.unwrap_or("NULL"),
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod attr_value_validation_tests {
    use super::*;
    use serde_json::json;

    // bug-hunt 2026-07-01: a non-key attribute with a malformed Number is a
    // ValidationException in real DynamoDB (it must not silently persist).
    #[test]
    fn rejects_invalid_number_attribute() {
        let item: HashMap<String, AttributeValue> =
            HashMap::from([("n".to_string(), json!({"N": "abc"}))]);
        assert!(validate_item_attribute_values(&item).is_err());
    }

    #[test]
    fn rejects_invalid_number_nested_in_list_and_map() {
        let item: HashMap<String, AttributeValue> = HashMap::from([(
            "l".to_string(),
            json!({"L": [{"N": "1"}, {"M": {"x": {"N": "oops"}}}]}),
        )]);
        assert!(validate_item_attribute_values(&item).is_err());
    }

    #[test]
    fn rejects_invalid_number_set_member() {
        let item: HashMap<String, AttributeValue> =
            HashMap::from([("ns".to_string(), json!({"NS": ["1", "x"]}))]);
        assert!(validate_item_attribute_values(&item).is_err());
    }

    #[test]
    fn accepts_valid_values() {
        let item: HashMap<String, AttributeValue> = HashMap::from([
            ("n".to_string(), json!({"N": "3.14"})),
            ("s".to_string(), json!({"S": "hi"})),
            ("ns".to_string(), json!({"NS": ["1", "2.5"]})),
        ]);
        assert!(validate_item_attribute_values(&item).is_ok());
    }

    // DynamoDB rejects a Number with more than 38 significant digits.
    #[test]
    fn rejects_number_over_38_significant_digits() {
        let thirty_nine = "1".repeat(39);
        let err = err_of(json!({ "N": thirty_nine }));
        assert_eq!(err.code(), "ValidationException");
        assert!(
            err.message().contains("38 significant digits"),
            "{}",
            err.message()
        );

        // Same limit inside a Number Set.
        let err = err_of(json!({ "NS": ["1", "9".repeat(39)] }));
        assert_eq!(err.code(), "ValidationException");
    }

    #[test]
    fn accepts_number_at_and_below_38_digit_limit() {
        // Exactly 38 significant digits is allowed.
        assert!(validate_attribute_value(&json!({ "N": "1".repeat(38) })).is_ok());
        // A 39-character string that is only 1 significant digit (1E38) is fine:
        // trailing zeros collapse into the exponent, not the coefficient.
        let one_e38 = format!("1{}", "0".repeat(38));
        assert!(validate_attribute_value(&json!({ "N": one_e38 })).is_ok());
        // Leading zeros in a fraction are likewise insignificant.
        let small = format!("0.{}1", "0".repeat(40));
        assert!(validate_attribute_value(&json!({ "N": small })).is_ok());
    }

    fn err_of(v: serde_json::Value) -> AwsServiceError {
        let item: HashMap<String, AttributeValue> = HashMap::from([("a".to_string(), v)]);
        validate_item_attribute_values(&item).unwrap_err()
    }

    #[test]
    fn rejects_empty_sets() {
        for (v, kind) in [
            (json!({"SS": []}), "string"),
            (json!({"NS": []}), "number"),
            (json!({"BS": []}), "binary"),
        ] {
            let err = err_of(v);
            assert_eq!(err.code(), "ValidationException");
            assert!(
                err.message()
                    .contains(&format!("An {kind} set may not be empty")),
                "unexpected message for {kind}: {}",
                err.message()
            );
        }
    }

    #[test]
    fn rejects_duplicate_string_set() {
        let err = err_of(json!({"SS": ["a", "a"]}));
        assert_eq!(err.code(), "ValidationException");
        assert!(
            err.message().contains("contains duplicates"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn rejects_numeric_value_duplicate_number_set() {
        // "1" and "1.0" are the same numeric value -> duplicate.
        let err = err_of(json!({"NS": ["1", "1.0"]}));
        assert_eq!(err.code(), "ValidationException");
        assert!(
            err.message().contains("contains duplicates"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn rejects_duplicate_binary_set() {
        // Both decode to the same bytes.
        let err = err_of(json!({"BS": ["aGVsbG8=", "aGVsbG8="]}));
        assert_eq!(err.code(), "ValidationException");
        assert!(
            err.message().contains("contains duplicates"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn accepts_valid_sets() {
        let item: HashMap<String, AttributeValue> = HashMap::from([
            ("ss".to_string(), json!({"SS": ["a", "b", "c"]})),
            ("ns".to_string(), json!({"NS": ["1", "2", "3.5"]})),
            ("bs".to_string(), json!({"BS": ["aGVsbG8=", "d29ybGQ="]})),
        ]);
        assert!(validate_item_attribute_values(&item).is_ok());
    }
}
