//! `LambdaService` `code_signing` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl LambdaService {
    // ── Code signing ──

    pub(super) fn create_code_signing_config(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        if body.get("AllowedPublishers").is_none_or(Value::is_null) {
            return Err(null_member("allowedPublishers"));
        }
        check_optional_length(&body, "Description", "description", 256)?;
        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        let id = new_csc_id();
        let arn = fakecloud_aws::arn::Arn::regional(
            "lambda",
            &req.region,
            &state.account_id,
            &format!("code-signing-config:{id}"),
        )
        .to_string();
        let publishers: Vec<String> = body
            .get("AllowedPublishers")
            .and_then(|v| v.get("SigningProfileVersionArns"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let csc = CodeSigningConfig {
            csc_id: id.clone(),
            csc_arn: arn,
            description: body["Description"].as_str().unwrap_or("").to_string(),
            allowed_publishers: publishers,
            untrusted_artifact_action: body["CodeSigningPolicies"]["UntrustedArtifactOnDeployment"]
                .as_str()
                .unwrap_or("Warn")
                .to_string(),
            last_modified: Utc::now(),
        };
        state.code_signing_configs.insert(id, csc.clone());
        ok(json!({"CodeSigningConfig": code_signing_json(&csc)}))
    }

    pub(super) fn get_code_signing_config(
        &self,
        csc_id: &str,
        account_id: &str,
        region: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = extract_csc_id(csc_id);
        let region = region.to_string();
        self.with_state_read(account_id, &region, |state| {
            state
                .code_signing_configs
                .get(&id)
                .map(|c| ok(json!({"CodeSigningConfig": code_signing_json(c)})))
                .unwrap_or_else(|| Err(not_found("CodeSigningConfig", &id)))
        })
    }

    pub(super) fn update_code_signing_config(
        &self,
        csc_id: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        let id = extract_csc_id(csc_id);
        let csc = state
            .code_signing_configs
            .get_mut(&id)
            .ok_or_else(|| not_found("CodeSigningConfig", &id))?;
        if let Some(d) = body["Description"].as_str() {
            csc.description = d.to_string();
        }
        if let Some(action) = body["CodeSigningPolicies"]["UntrustedArtifactOnDeployment"].as_str()
        {
            csc.untrusted_artifact_action = action.to_string();
        }
        csc.last_modified = Utc::now();
        ok(json!({"CodeSigningConfig": code_signing_json(csc)}))
    }

    pub(super) fn delete_code_signing_config(
        &self,
        csc_id: &str,
        account_id: &str,
        region: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_csc_arn(csc_id)?;
        let id = extract_csc_id(csc_id);
        let mut accounts = self.state.write();
        let state = accounts.regional_mut(account_id, region);
        if state.code_signing_configs.remove(&id).is_none() {
            return Err(not_found("CodeSigningConfig", &id));
        }
        empty()
    }

    pub(super) fn list_code_signing_configs(
        &self,
        account_id: &str,
        region: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let region = region.to_string();
        self.with_state_read(account_id, &region, |state| {
            let cfgs: Vec<Value> = state
                .code_signing_configs
                .values()
                .map(code_signing_json)
                .collect();
            ok(json!({"CodeSigningConfigs": cfgs}))
        })
    }

    pub(super) fn put_function_code_signing(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        let csc_arn = body["CodeSigningConfigArn"]
            .as_str()
            .ok_or_else(|| missing("CodeSigningConfigArn"))?
            .to_string();
        // Smithy length bound: max 200. Reject overlong inputs rather
        // than persisting a malformed ARN.
        if csc_arn.chars().count() > 200 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                "CodeSigningConfigArn exceeds the 200-character maximum",
            ));
        }
        let mut accounts = self.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
        state
            .function_code_signing
            .insert(function_name.to_string(), csc_arn.clone());
        ok(json!({
            "CodeSigningConfigArn": csc_arn,
            "FunctionName": function_name,
        }))
    }

    pub(super) fn get_function_code_signing(
        &self,
        function_name: &str,
        account_id: &str,
        region: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let region = region.to_string();
        self.with_state_read(account_id, &region, |state| {
            let arn = state
                .function_code_signing
                .get(function_name)
                .cloned()
                .unwrap_or_default();
            ok(json!({
                "CodeSigningConfigArn": arn,
                "FunctionName": function_name,
            }))
        })
    }

    pub(super) fn delete_function_code_signing(
        &self,
        function_name: &str,
        account_id: &str,
        region: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.regional_mut(account_id, region);
        state.function_code_signing.remove(function_name);
        empty()
    }

    pub(super) fn list_functions_by_code_signing(
        &self,
        csc_id: &str,
        account_id: &str,
        region: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_csc_arn(csc_id)?;
        let id = extract_csc_id(csc_id);
        let region = region.to_string();
        self.with_state_read(account_id, &region, |state| {
            if !state.code_signing_configs.contains_key(&id) {
                return Err(not_found("CodeSigningConfig", &id));
            }
            // Exact id match — substring matching would surface
            // functions bound to a different csc that happens to share
            // a prefix/suffix.
            let funcs: Vec<&String> = state
                .function_code_signing
                .iter()
                .filter(|(_, v)| extract_csc_id(v) == id)
                .map(|(k, _)| k)
                .collect();
            ok(json!({"FunctionArns": funcs}))
        })
    }
}
