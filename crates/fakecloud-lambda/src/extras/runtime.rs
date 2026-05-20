//! `LambdaService` `runtime` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl LambdaService {
    // ── Runtime management ──

    pub(super) fn put_runtime_management(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        let qualifier = parse_qualifier(req);
        // `UpdateRuntimeOn` is `@required` in the model; reject the
        // request rather than silently defaulting to `Auto`.
        let update_runtime_on = body["UpdateRuntimeOn"]
            .as_str()
            .ok_or_else(|| missing("UpdateRuntimeOn"))?
            .to_string();
        // `UpdateRuntimeOn` enum: Auto | Manual | FunctionUpdate.
        if !matches!(
            update_runtime_on.as_str(),
            "Auto" | "Manual" | "FunctionUpdate"
        ) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                format!(
                    "Invalid UpdateRuntimeOn value '{}'; expected 'Auto', 'Manual', or 'FunctionUpdate'",
                    update_runtime_on
                ),
            ));
        }
        // Reject non-string RuntimeVersionArn values instead of silently
        // coercing to "". The Smithy shape is @length(26, 2048) on a
        // string; passing a number / array / null is a 400 input error.
        let runtime_version_arn = match &body["RuntimeVersionArn"] {
            serde_json::Value::Null => String::new(),
            serde_json::Value::String(s) => s.clone(),
            _ => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    "RuntimeVersionArn must be a string",
                ));
            }
        };
        // `RuntimeVersionArn` Smithy shape: length 26..2048. Empty
        // means "unset" (valid); any non-empty value must satisfy the
        // minimum.
        if !runtime_version_arn.is_empty()
            && (runtime_version_arn.chars().count() < 26
                || runtime_version_arn.chars().count() > 2048)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                "RuntimeVersionArn must be 26..2048 characters",
            ));
        }
        let cfg = RuntimeManagementConfig {
            update_runtime_on,
            runtime_version_arn,
        };
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .runtime_management
            .insert(format!("{function_name}:{qualifier}"), cfg.clone());
        ok(json!({
            "FunctionArn": Arn::new("lambda", &state.region, &state.account_id, &format!("function:{function_name}:{qualifier}")).to_string(),
            "UpdateRuntimeOn": cfg.update_runtime_on,
            "RuntimeVersionArn": cfg.runtime_version_arn,
        }))
    }

    pub(super) fn get_runtime_management(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let qualifier = parse_qualifier(req);
        let region = self.region_for(&req.account_id);
        self.with_state_read(&req.account_id, &region, |state| {
            let cfg = state
                .runtime_management
                .get(&format!("{function_name}:{qualifier}"))
                .cloned()
                .unwrap_or(RuntimeManagementConfig {
                    update_runtime_on: "Auto".to_string(),
                    runtime_version_arn: String::new(),
                });
            ok(json!({
                "FunctionArn": format!(
                    "arn:aws:lambda:{}:{}:function:{}:{}",
                    state.region, state.account_id, function_name, qualifier
                ),
                "UpdateRuntimeOn": cfg.update_runtime_on,
                "RuntimeVersionArn": cfg.runtime_version_arn,
            }))
        })
    }
}
