//! Account-level data retention configuration.
//!
//! `GetAccountDataRetention` reports the account's data retention mode,
//! defaulting to `default` when none has been set. `PutAccountDataRetention`
//! records a new mode. Valid modes mirror the `DataRetentionMode` enum.

use chrono::Utc;
use http::StatusCode;
use serde_json::json;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{DataRetentionConfig, SharedBedrockState};

const VALID_MODES: [&str; 4] = ["default", "none", "provider_data_share", "inherit"];

pub(crate) fn get_account_data_retention(
    state: &SharedBedrockState,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let accts = state.read();
    let config = accts
        .get(&req.account_id)
        .and_then(|s| s.data_retention.clone());
    let body = match config {
        Some(c) => json!({
            "mode": c.mode,
            "updatedAt": c.updated_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        }),
        None => json!({ "mode": "default" }),
    };
    Ok(AwsResponse::ok_json(body))
}

pub(crate) fn put_account_data_retention(
    state: &SharedBedrockState,
    req: &AwsRequest,
    body: &serde_json::Value,
) -> Result<AwsResponse, AwsServiceError> {
    let mode = body.get("mode").and_then(|v| v.as_str()).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "mode is required",
        )
    })?;
    if !VALID_MODES.contains(&mode) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!(
                "Value '{mode}' at 'mode' failed to satisfy constraint: Member must satisfy enum value set: [{}]",
                VALID_MODES.join(", ")
            ),
        ));
    }

    let updated_at = Utc::now();
    let mut accts = state.write();
    let s = accts.get_or_create(&req.account_id);
    s.data_retention = Some(DataRetentionConfig {
        mode: mode.to_string(),
        updated_at,
    });
    Ok(AwsResponse::ok_json(json!({
        "mode": mode,
        "updatedAt": updated_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
    })))
}
