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
        // Absent key (no entry in the JSON body) is allowed and means
        // "leave unset"; explicit `null` is not the same — AWS treats
        // it as a malformed enum value.
        let runtime_version_arn = if body.get("RuntimeVersionArn").is_none() {
            String::new()
        } else {
            match &body["RuntimeVersionArn"] {
                serde_json::Value::String(s) => s.clone(),
                _ => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValueException",
                        "RuntimeVersionArn must be a string",
                    ));
                }
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
        let mut resp = json!({
            "FunctionArn": Arn::new("lambda", &state.region, &state.account_id, &format!("function:{function_name}:{qualifier}")).to_string(),
            "UpdateRuntimeOn": cfg.update_runtime_on,
        });
        // RuntimeVersionArn is an ARN-typed field; an empty value fails the
        // SDK's client-side ARN validation, so omit it when unset (Auto /
        // FunctionUpdate modes carry no pinned runtime version).
        if !cfg.runtime_version_arn.is_empty() {
            resp["RuntimeVersionArn"] = json!(cfg.runtime_version_arn);
        }
        ok(resp)
    }

    pub(super) fn get_runtime_management(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let qualifier = parse_qualifier(req);
        let region = self.region_for(&req.account_id);
        self.with_state_read(&req.account_id, &region, |state| {
            // The config only exists while the function does; once the function
            // is deleted GetRuntimeManagementConfig must 404 (the Terraform
            // CheckDestroy relies on this), not synthesise a default.
            if !state.functions.contains_key(function_name) {
                return Err(not_found("Function", function_name));
            }
            let cfg = state
                .runtime_management
                .get(&format!("{function_name}:{qualifier}"))
                .cloned()
                .unwrap_or(RuntimeManagementConfig {
                    update_runtime_on: "Auto".to_string(),
                    runtime_version_arn: String::new(),
                });
            let mut resp = json!({
                "FunctionArn": format!(
                    "arn:aws:lambda:{}:{}:function:{}:{}",
                    state.region, state.account_id, function_name, qualifier
                ),
                "UpdateRuntimeOn": cfg.update_runtime_on,
            });
            if !cfg.runtime_version_arn.is_empty() {
                resp["RuntimeVersionArn"] = json!(cfg.runtime_version_arn);
            }
            ok(resp)
        })
    }
}
