//! apigateway data_plane `validator` concerns (audit-2026-05-19).

use super::*;

/// Compute the canonical window key for a quota period. AWS resets
/// quota counters at the start of the period (UTC midnight for DAY,
/// Sunday for WEEK, first-of-month for MONTH); `offset` shifts the
/// boundary by N days. The returned string is purely a counter key.
#[allow(clippy::too_many_arguments)]
pub(super) fn enforce_request_validator(
    service: &ApiGatewayService,
    req: &AwsRequest,
    api_id: &str,
    _stage_name: &str,
    validator_id: &str,
    request_parameters: &BTreeMap<String, bool>,
    request_models: &BTreeMap<String, String>,
    path_params: &BTreeMap<String, String>,
) -> Result<(), AwsServiceError> {
    let accounts = service.state_handle().read();
    let state = accounts
        .get(&req.account_id)
        .ok_or_else(|| bad_request("Request validation failed"))?;

    let validator = state
        .request_validators
        .get(api_id)
        .and_then(|m| m.get(validator_id))
        .ok_or_else(|| bad_request("Request validation failed"))?;

    let validate_body = validator
        .get("validateRequestBody")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let validate_params = validator
        .get("validateRequestParameters")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    if validate_params {
        for (param_key, required) in request_parameters {
            if !*required {
                continue;
            }
            let present = if let Some(name) = param_key.strip_prefix("method.request.querystring.")
            {
                req.query_params
                    .get(name)
                    .map(|v| !v.is_empty())
                    .unwrap_or(false)
            } else if let Some(name) = param_key.strip_prefix("method.request.path.") {
                path_params
                    .get(name)
                    .map(|v| !v.is_empty())
                    .unwrap_or(false)
            } else if let Some(name) = param_key.strip_prefix("method.request.header.") {
                req.headers
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .map(|v| !v.trim().is_empty())
                    .unwrap_or(false)
            } else {
                // Unknown parameter source — skip.
                true
            };
            if !present {
                return Err(bad_request(format!(
                    "Missing required request parameter: {}",
                    param_key
                )));
            }
        }
    }

    if validate_body {
        let content_type = req
            .headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/json");
        // Normalize content-type by stripping charset suffix.
        let normalized = content_type
            .split(';')
            .next()
            .unwrap_or(content_type)
            .trim();

        let model_name = match request_models.get(normalized) {
            Some(name) => name,
            None => match request_models.get("$default") {
                Some(name) => name,
                None => {
                    // No model for this content type and no $default — skip body validation.
                    return Ok(());
                }
            },
        };

        let model = state
            .models
            .get(api_id)
            .and_then(|m| m.get(model_name))
            .ok_or_else(|| bad_request("Request body does not match model schema"))?;

        let schema_str = model
            .schema
            .as_deref()
            .ok_or_else(|| bad_request("Request body does not match model schema"))?;
        let schema: serde_json::Value = serde_json::from_str(schema_str)
            .map_err(|_| bad_request("Request body does not match model schema"))?;

        let body_json: serde_json::Value = serde_json::from_slice(&req.body)
            .map_err(|_| bad_request("Request body does not match model schema"))?;

        drop(accounts);

        if let Err(e) = crate::model_validation::validate(&schema, &body_json) {
            return Err(bad_request(format!(
                "Request body does not match model schema for content type {}: {}",
                normalized, e
            )));
        }
    }

    Ok(())
}
