//! `LambdaService` `function_url` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl LambdaService {
    // ── Function URL ──

    /// Render a `FunctionUrlConfig` into the AWS-shaped JSON the Lambda
    /// SDK expects (PascalCase keys, ISO-8601 timestamps). Direct
    /// `serde_json::to_value` would emit the struct's snake_case field
    /// names, which the SDK silently treats as missing fields — leaving
    /// `function_url()` returning an empty string.
    pub(super) fn function_url_config_json(cfg: &FunctionUrlConfig) -> Value {
        let mut out = json!({
            "FunctionArn": cfg.function_arn,
            "FunctionUrl": cfg.function_url,
            "AuthType": cfg.auth_type,
            "InvokeMode": cfg.invoke_mode,
            "CreationTime": cfg.creation_time.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string(),
            "LastModifiedTime": cfg.last_modified_time.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string(),
        });
        if let Some(cors) = &cfg.cors {
            out["Cors"] = cors.clone();
        }
        out
    }

    pub(super) fn create_function_url_config(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        let auth_type = body["AuthType"]
            .as_str()
            .ok_or_else(|| missing("AuthType"))?
            .to_string();
        // `FunctionUrlAuthType` enum: `NONE` | `AWS_IAM`. Reject any
        // other value rather than persisting an unrecognised auth type.
        if auth_type != "NONE" && auth_type != "AWS_IAM" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                format!(
                    "Invalid AuthType value '{}'; expected 'NONE' or 'AWS_IAM'",
                    auth_type
                ),
            ));
        }
        let now = Utc::now();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.functions.contains_key(function_name) {
            return Err(not_found("Function", function_name));
        }
        // Derive the FunctionArn and the function URL's region label from the
        // request's credential-scope region (`req.region`), matching the
        // function's own ARN, not the server default (`state.region`). Both
        // are persisted and re-emitted by Get/Update/List.
        let function_arn = format!(
            "arn:aws:lambda:{}:{}:function:{}",
            req.region, state.account_id, function_name
        );
        let cfg = FunctionUrlConfig {
            function_arn: function_arn.clone(),
            function_url: format!("https://{function_name}.lambda-url.{}.on.aws/", req.region),
            auth_type: auth_type.clone(),
            cors: body.get("Cors").cloned(),
            creation_time: now,
            last_modified_time: now,
            invoke_mode: {
                let m = body["InvokeMode"]
                    .as_str()
                    .unwrap_or("BUFFERED")
                    .to_string();
                if m != "BUFFERED" && m != "RESPONSE_STREAM" {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValueException",
                        format!(
                            "Invalid InvokeMode value '{}'; expected 'BUFFERED' or 'RESPONSE_STREAM'",
                            m
                        ),
                    ));
                }
                m
            },
        };
        state
            .function_url_configs
            .insert(function_name.to_string(), cfg.clone());
        // `CreateFunctionUrlConfigResponse` lacks `LastModifiedTime` —
        // that member only appears on `Get`/`Update` responses. Strip it
        // before returning so strict shape validators don't reject it.
        let mut created = Self::function_url_config_json(&cfg);
        if let Some(obj) = created.as_object_mut() {
            obj.remove("LastModifiedTime");
        }
        ok(created)
    }

    pub(super) fn get_function_url_config(
        &self,
        function_name: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let region = self.region_for(account_id);
        self.with_state_read(account_id, &region, |state| {
            state
                .function_url_configs
                .get(function_name)
                .map(|c| ok(Self::function_url_config_json(c)))
                .unwrap_or_else(|| Err(not_found("FunctionUrlConfig", function_name)))
        })
    }

    pub(super) fn update_function_url_config(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let cfg = state
            .function_url_configs
            .get_mut(function_name)
            .ok_or_else(|| not_found("FunctionUrlConfig", function_name))?;
        if let Some(a) = body["AuthType"].as_str() {
            if a != "NONE" && a != "AWS_IAM" {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    format!("AuthType must be NONE or AWS_IAM, got '{a}'"),
                ));
            }
            cfg.auth_type = a.to_string();
        }
        if let Some(c) = body.get("Cors") {
            cfg.cors = Some(c.clone());
        }
        if let Some(m) = body["InvokeMode"].as_str() {
            if m != "BUFFERED" && m != "RESPONSE_STREAM" {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    format!("InvokeMode must be BUFFERED or RESPONSE_STREAM, got '{m}'"),
                ));
            }
            cfg.invoke_mode = m.to_string();
        }
        cfg.last_modified_time = Utc::now();
        let snapshot = cfg.clone();
        ok(Self::function_url_config_json(&snapshot))
    }

    pub(super) fn delete_function_url_config(
        &self,
        function_name: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        state.function_url_configs.remove(function_name);
        empty()
    }

    pub(super) fn list_function_url_configs(
        &self,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let region = self.region_for(account_id);
        self.with_state_read(account_id, &region, |state| {
            let configs: Vec<Value> = state
                .function_url_configs
                .values()
                .map(Self::function_url_config_json)
                .collect();
            ok(json!({"FunctionUrlConfigs": configs}))
        })
    }
}
