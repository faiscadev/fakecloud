//! dynamodb helpers `schemas` concerns (audit-2026-05-19).

use super::*;

pub(crate) fn parse_key_schema(val: &Value) -> Result<Vec<KeySchemaElement>, AwsServiceError> {
    let arr = val.as_array().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "KeySchema is required",
        )
    })?;
    let err = |m: &str| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            m.to_string(),
        )
    };
    // A DynamoDB key schema has exactly one HASH element and at most one
    // RANGE element (1 or 2 total). Previously the schema was parsed
    // permissively: a missing KeyType defaulted to HASH, an empty
    // AttributeName was accepted, and a two-HASH / zero-HASH / oversized
    // schema was stored as-is — all of which real DDB rejects with a
    // ValidationException (bug-hunt 2026-07-01, finding 7).
    if arr.is_empty() || arr.len() > 2 {
        return Err(err(
            "1 validation error detected: KeySchema must contain 1 or 2 elements",
        ));
    }
    let mut elements = Vec::with_capacity(arr.len());
    let mut hash_count = 0usize;
    for elem in arr {
        let attribute_name = elem["AttributeName"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if attribute_name.is_empty() {
            return Err(err(
                "1 validation error detected: KeySchema AttributeName must not be empty",
            ));
        }
        let key_type = match elem["KeyType"].as_str() {
            Some("HASH") => "HASH",
            Some("RANGE") => "RANGE",
            _ => {
                return Err(err(
                    "1 validation error detected: Value at 'keySchema' failed to satisfy \
                     constraint: KeyType must be one of HASH or RANGE",
                ));
            }
        };
        if key_type == "HASH" {
            hash_count += 1;
        }
        elements.push(KeySchemaElement {
            attribute_name,
            key_type: key_type.to_string(),
        });
    }
    if hash_count != 1 {
        return Err(err(
            "1 validation error detected: KeySchema must specify exactly one HASH key",
        ));
    }
    Ok(elements)
}

/// Validate `GlobalSecondaryIndexes` / `LocalSecondaryIndexes` definitions on
/// CreateTable. Each index must carry a non-empty `IndexName`, a well-formed
/// `KeySchema` (validated through [`parse_key_schema`], which rejects a
/// non-array or structurally-invalid schema), and every one of its key
/// attributes must be declared in the table's AttributeDefinitions. Real DDB
/// returns a ValidationException for any of these; `parse_gsi`/`parse_lsi`
/// otherwise SILENTLY DROP a malformed index (bug-hunt 2026-07-01, finding 6).
pub(crate) fn validate_index_definitions(
    gsi_val: &Value,
    lsi_val: &Value,
    attribute_definitions: &[AttributeDefinition],
) -> Result<(), AwsServiceError> {
    for (val, label) in [
        (gsi_val, "GlobalSecondaryIndexes"),
        (lsi_val, "LocalSecondaryIndexes"),
    ] {
        let Some(arr) = val.as_array() else { continue };
        for idx in arr {
            if idx["IndexName"]
                .as_str()
                .filter(|s| !s.is_empty())
                .is_none()
            {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!(
                        "One or more parameter values were invalid: {label} entry is missing IndexName"
                    ),
                ));
            }
            // Rejects a non-array / structurally-invalid index KeySchema.
            let key_schema = parse_key_schema(&idx["KeySchema"])?;
            for ks in &key_schema {
                if !attribute_definitions
                    .iter()
                    .any(|ad| ad.attribute_name == ks.attribute_name)
                {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ValidationException",
                        format!(
                            "One or more parameter values were invalid: \
                             Some index key attributes are not defined in AttributeDefinitions. \
                             Keys: [{}], AttributeDefinitions: [{}]",
                            ks.attribute_name,
                            attribute_definitions
                                .iter()
                                .map(|ad| ad.attribute_name.as_str())
                                .collect::<Vec<_>>()
                                .join(", ")
                        ),
                    ));
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn parse_attribute_definitions(
    val: &Value,
) -> Result<Vec<AttributeDefinition>, AwsServiceError> {
    let arr = val.as_array().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "AttributeDefinitions is required",
        )
    })?;
    Ok(arr
        .iter()
        .map(|elem| AttributeDefinition {
            attribute_name: elem["AttributeName"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            attribute_type: elem["AttributeType"].as_str().unwrap_or("S").to_string(),
        })
        .collect())
}

pub(crate) fn parse_provisioned_throughput(
    val: &Value,
) -> Result<ProvisionedThroughput, AwsServiceError> {
    Ok(ProvisionedThroughput {
        read_capacity_units: val["ReadCapacityUnits"].as_i64().unwrap_or(5),
        write_capacity_units: val["WriteCapacityUnits"].as_i64().unwrap_or(5),
    })
}

pub fn parse_gsi(val: &Value, billing_mode: &str) -> Vec<GlobalSecondaryIndex> {
    let Some(arr) = val.as_array() else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|g| {
            Some(GlobalSecondaryIndex {
                index_name: g["IndexName"].as_str()?.to_string(),
                key_schema: parse_key_schema(&g["KeySchema"]).ok()?,
                projection: parse_projection(&g["Projection"]),
                provisioned_throughput: Some(parse_gsi_throughput(
                    &g["ProvisionedThroughput"],
                    billing_mode,
                )),
                on_demand_throughput: parse_on_demand_throughput(&g["OnDemandThroughput"]),
            })
        })
        .collect()
}

/// Resolve the provisioned-throughput slot for a GSI on a CreateTable or
/// UpdateTable Create action. Real DynamoDB returns `{0, 0}` for GSIs on
/// PAY_PER_REQUEST tables regardless of whether the caller sent a
/// `ProvisionedThroughput` block, and the Terraform provider's `flatten`
/// code keys `name`/`read_capacity`/`write_capacity` off the presence of
/// that field — returning `None` would desynchronise state.
pub fn parse_gsi_throughput(val: &Value, billing_mode: &str) -> ProvisionedThroughput {
    if billing_mode == "PAY_PER_REQUEST" {
        return ProvisionedThroughput {
            read_capacity_units: 0,
            write_capacity_units: 0,
        };
    }
    ProvisionedThroughput {
        read_capacity_units: val["ReadCapacityUnits"].as_i64().unwrap_or(5),
        write_capacity_units: val["WriteCapacityUnits"].as_i64().unwrap_or(5),
    }
}

pub fn parse_lsi(val: &Value) -> Vec<LocalSecondaryIndex> {
    let Some(arr) = val.as_array() else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|l| {
            Some(LocalSecondaryIndex {
                index_name: l["IndexName"].as_str()?.to_string(),
                key_schema: parse_key_schema(&l["KeySchema"]).ok()?,
                projection: parse_projection(&l["Projection"]),
            })
        })
        .collect()
}

pub fn parse_tags(val: &Value) -> BTreeMap<String, String> {
    let mut tags = BTreeMap::new();
    if let Some(arr) = val.as_array() {
        for tag in arr {
            if let (Some(k), Some(v)) = (tag["Key"].as_str(), tag["Value"].as_str()) {
                tags.insert(k.to_string(), v.to_string());
            }
        }
    }
    tags
}

pub(crate) fn parse_expression_attribute_names(body: &Value) -> HashMap<String, String> {
    let mut names = HashMap::new();
    if let Some(obj) = body["ExpressionAttributeNames"].as_object() {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                names.insert(k.clone(), s.to_string());
            }
        }
    }
    names
}

pub(crate) fn parse_expression_attribute_values(body: &Value) -> HashMap<String, Value> {
    let mut values = HashMap::new();
    if let Some(obj) = body["ExpressionAttributeValues"].as_object() {
        for (k, v) in obj {
            values.insert(k.clone(), v.clone());
        }
    }
    values
}

#[cfg(test)]
mod schema_validation_tests {
    use super::*;
    use serde_json::json;

    // bug-hunt 2026-07-01, finding 7: key-schema structure is validated.
    #[test]
    fn parse_key_schema_rejects_missing_keytype() {
        assert!(parse_key_schema(&json!([{"AttributeName": "pk"}])).is_err());
    }

    #[test]
    fn parse_key_schema_rejects_two_hash() {
        assert!(parse_key_schema(&json!([
            {"AttributeName": "a", "KeyType": "HASH"},
            {"AttributeName": "b", "KeyType": "HASH"}
        ]))
        .is_err());
    }

    #[test]
    fn parse_key_schema_rejects_empty_oversized_and_bad_name() {
        assert!(parse_key_schema(&json!([])).is_err());
        assert!(parse_key_schema(&json!([
            {"AttributeName": "a", "KeyType": "HASH"},
            {"AttributeName": "b", "KeyType": "RANGE"},
            {"AttributeName": "c", "KeyType": "RANGE"}
        ]))
        .is_err());
        assert!(parse_key_schema(&json!([{"AttributeName": "", "KeyType": "HASH"}])).is_err());
    }

    #[test]
    fn parse_key_schema_accepts_valid_composite() {
        let ks = parse_key_schema(&json!([
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"}
        ]))
        .unwrap();
        assert_eq!(ks.len(), 2);
    }

    // bug-hunt 2026-07-01, finding 6: index key attrs validated against defs,
    // malformed index rejected rather than silently dropped.
    #[test]
    fn validate_index_definitions_rejects_undefined_attr() {
        let defs = vec![AttributeDefinition {
            attribute_name: "pk".into(),
            attribute_type: "S".into(),
        }];
        let gsi = json!([{
            "IndexName": "g",
            "KeySchema": [{"AttributeName": "ghost", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}
        }]);
        let err = validate_index_definitions(&gsi, &Value::Null, &defs).unwrap_err();
        assert!(format!("{err:?}").contains("not defined in AttributeDefinitions"));
    }

    #[test]
    fn validate_index_definitions_rejects_missing_index_name() {
        let defs = vec![AttributeDefinition {
            attribute_name: "pk".into(),
            attribute_type: "S".into(),
        }];
        let gsi = json!([{"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]}]);
        assert!(validate_index_definitions(&gsi, &Value::Null, &defs).is_err());
    }

    #[test]
    fn validate_index_definitions_accepts_valid() {
        let defs = vec![
            AttributeDefinition {
                attribute_name: "pk".into(),
                attribute_type: "S".into(),
            },
            AttributeDefinition {
                attribute_name: "gpk".into(),
                attribute_type: "S".into(),
            },
        ];
        let gsi = json!([{
            "IndexName": "g",
            "KeySchema": [{"AttributeName": "gpk", "KeyType": "HASH"}],
            "Projection": {"ProjectionType": "ALL"}
        }]);
        assert!(validate_index_definitions(&gsi, &Value::Null, &defs).is_ok());
    }
}
