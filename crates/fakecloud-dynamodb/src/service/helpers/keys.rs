//! dynamodb helpers `keys` concerns (audit-2026-05-19).

use super::*;

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
