use std::collections::BTreeMap;

use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::CognitoService;

/// Canonical key for a `LimitDefinition` so the same definition always maps to
/// the same stored provisioned value regardless of attribute ordering in the
/// request JSON. Shape: `<LimitClass>|<k=v>,<k=v>...` with attributes sorted.
fn limit_definition_key(class: &str, attributes: &BTreeMap<String, String>) -> String {
    let attrs = attributes
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("{class}|{attrs}")
}

/// Parse and validate a `LimitDefinition` from a request body, returning the
/// `LimitClass` string and its (sorted) attribute map. `LimitClass` currently
/// only accepts `API_CATEGORY`, matching the Smithy `LimitClass` enum.
fn parse_limit_definition(
    def: &Value,
) -> Result<(String, BTreeMap<String, String>), AwsServiceError> {
    if !def.is_object() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterException",
            "LimitDefinition is required",
        ));
    }

    let class = def["LimitClass"].as_str().unwrap_or("").to_string();
    if class.is_empty() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterException",
            "LimitDefinition.LimitClass is required",
        ));
    }
    if class != "API_CATEGORY" {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterException",
            format!("Value '{class}' at 'limitDefinition.limitClass' failed to satisfy constraint: Member must satisfy enum value set: [API_CATEGORY]"),
        ));
    }

    let mut attributes = BTreeMap::new();
    if let Some(obj) = def["Attributes"].as_object() {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                attributes.insert(k.clone(), s.to_string());
            }
        }
    }

    Ok((class, attributes))
}

/// Build the `LimitType` response shape for a definition and its stored
/// provisioned value. `FreeLimitValue` is the always-free baseline; fakecloud
/// does not meter usage, so it is reported as 0 (the account pays for every
/// unit of `ProvisionedLimitValue`).
fn limit_type_json(class: &str, attributes: &BTreeMap<String, String>, provisioned: i64) -> Value {
    json!({
        "LimitDefinition": {
            "LimitClass": class,
            "Attributes": attributes,
        },
        "ProvisionedLimitValue": provisioned,
        "FreeLimitValue": 0,
    })
}

impl CognitoService {
    pub(super) fn get_provisioned_limit(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let (class, attributes) = parse_limit_definition(&body["LimitDefinition"])?;
        let key = limit_definition_key(&class, &attributes);

        let accounts = self.state.read();
        let provisioned = accounts
            .get(&req.account_id)
            .and_then(|state| state.provisioned_limits.get(&key).copied())
            .unwrap_or(0);

        Ok(AwsResponse::ok_json(json!({
            "Limit": limit_type_json(&class, &attributes, provisioned),
        })))
    }

    pub(super) fn update_provisioned_limit(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let (class, attributes) = parse_limit_definition(&body["LimitDefinition"])?;

        let requested = body["RequestedLimitValue"].as_i64().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "RequestedLimitValue is required",
            )
        })?;
        if requested < 0 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "Value at 'requestedLimitValue' failed to satisfy constraint: Member must be greater than or equal to 0",
            ));
        }

        let key = limit_definition_key(&class, &attributes);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.provisioned_limits.insert(key, requested);

        Ok(AwsResponse::ok_json(json!({
            "Limit": limit_type_json(&class, &attributes, requested),
        })))
    }
}
