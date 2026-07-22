//! `LambdaService` `concurrency` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl LambdaService {
    // ── Concurrency ──

    pub(super) fn put_function_concurrency(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        let n = body["ReservedConcurrentExecutions"]
            .as_i64()
            .ok_or_else(|| missing("ReservedConcurrentExecutions"))?;
        // Smithy `range(min=0)` — negative values are invalid.
        if n < 0 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                format!("ReservedConcurrentExecutions must be >= 0 (got {})", n),
            ));
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // AWS returns ResourceNotFoundException for a config-put against a
        // function that doesn't exist; get_or_create only makes the account,
        // so guard explicitly or we'd store a ghost config the function never
        // owns.
        if !state.functions.contains_key(function_name) {
            return Err(not_found("Function", function_name));
        }
        state
            .function_concurrency
            .insert(function_name.to_string(), n);
        ok(json!({"ReservedConcurrentExecutions": n}))
    }

    pub(super) fn get_function_concurrency(
        &self,
        function_name: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let region = self.region_for(account_id);
        self.with_state_read(account_id, &region, |state| {
            // No reserved concurrency configured -> AWS returns an empty body,
            // not `ReservedConcurrentExecutions: 0` (which means "throttle to
            // zero", the opposite of unset).
            match state.function_concurrency.get(function_name).copied() {
                Some(n) => ok(json!({ "ReservedConcurrentExecutions": n })),
                None => ok(json!({})),
            }
        })
    }

    pub(super) fn delete_function_concurrency(
        &self,
        function_name: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        state.function_concurrency.remove(function_name);
        empty()
    }

    pub(super) fn put_provisioned_concurrency(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        let qualifier = require_qualifier(req)?;
        let requested = body["ProvisionedConcurrentExecutions"]
            .as_i64()
            .ok_or_else(|| missing("ProvisionedConcurrentExecutions"))?;
        // Smithy `range(min=1)` — zero and negatives are invalid.
        if requested < 1 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                format!(
                    "ProvisionedConcurrentExecutions must be >= 1 (got {})",
                    requested
                ),
            ));
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.functions.contains_key(function_name) {
            return Err(not_found("Function", function_name));
        }
        let cfg = ProvisionedConcurrencyConfig {
            requested,
            allocated: requested,
            status: "READY".to_string(),
            last_modified: Utc::now(),
        };
        state
            .provisioned_concurrency
            .insert(Self::pc_key(function_name, &qualifier), cfg.clone());
        ok(json!({
            "RequestedProvisionedConcurrentExecutions": cfg.requested,
            "AvailableProvisionedConcurrentExecutions": cfg.allocated,
            "AllocatedProvisionedConcurrentExecutions": cfg.allocated,
            "Status": cfg.status,
            "LastModified": cfg.last_modified.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string(),
        }))
    }

    pub(super) fn get_provisioned_concurrency(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let qualifier = require_qualifier(req)?;
        let region = self.region_for(&req.account_id);
        self.with_state_read(&req.account_id, &region, |state| {
            state
                .provisioned_concurrency
                .get(&Self::pc_key(function_name, &qualifier))
                .map(|cfg| ok(json!({
                    "RequestedProvisionedConcurrentExecutions": cfg.requested,
                    "AvailableProvisionedConcurrentExecutions": cfg.allocated,
                    "AllocatedProvisionedConcurrentExecutions": cfg.allocated,
                    "Status": cfg.status,
                    "LastModified": cfg.last_modified.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string(),
                })))
                .unwrap_or_else(|| Err(not_found("ProvisionedConcurrencyConfig", function_name)))
        })
    }

    pub(super) fn delete_provisioned_concurrency(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let qualifier = require_qualifier(req)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .provisioned_concurrency
            .remove(&Self::pc_key(function_name, &qualifier));
        empty()
    }

    pub(super) fn list_provisioned_concurrency(
        &self,
        function_name: &str,
        account_id: &str,
        region: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state_region = self.region_for(account_id);
        self.with_state_read(account_id, &state_region, |state| {
            let prefix = format!("{function_name}:");
            let configs: Vec<Value> = state
                .provisioned_concurrency
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .map(|(k, cfg)| {
                    let qualifier = k.split(':').next_back().unwrap_or("$LATEST");
                    json!({
                        "FunctionArn": format!(
                            "arn:aws:lambda:{}:{}:function:{}:{}",
                            region, state.account_id, function_name, qualifier
                        ),
                        "Status": cfg.status,
                        "RequestedProvisionedConcurrentExecutions": cfg.requested,
                        "AvailableProvisionedConcurrentExecutions": cfg.allocated,
                        "AllocatedProvisionedConcurrentExecutions": cfg.allocated,
                        "LastModified": cfg.last_modified.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string(),
                    })
                })
                .collect();
            ok(json!({"ProvisionedConcurrencyConfigs": configs}))
        })
    }

    // ── Scaling ──

    pub(super) fn put_scaling_config(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let qualifier = require_qualifier(req)?;
        let body = body(req);
        let inner = body
            .get("FunctionScalingConfig")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let cfg = FunctionScalingConfig {
            min_execution_environments: inner["MinExecutionEnvironments"].as_i64(),
            max_execution_environments: inner["MaxExecutionEnvironments"].as_i64(),
        };
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.functions.contains_key(function_name) {
            return Err(not_found("Function", function_name));
        }
        // Scaling config is per-qualifier (version/alias); keying by bare
        // function name collided distinct versions onto one entry.
        state
            .scaling_configs
            .insert(Self::pc_key(function_name, &qualifier), cfg);
        // `PutFunctionScalingConfigResponse` only carries `FunctionState`
        // (the post-update steady state). Pending → ready is instant in
        // fakecloud since there's no real fleet to scale.
        ok(json!({ "FunctionState": "Ready" }))
    }

    pub(super) fn get_scaling_config(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let qualifier = require_qualifier(req)?;
        let account_id = &req.account_id;
        let region = self.region_for(account_id);
        let key = Self::pc_key(function_name, &qualifier);
        self.with_state_read(account_id, &region, |state| {
            let cfg = state.scaling_configs.get(&key).cloned().unwrap_or_default();
            let mut applied = serde_json::Map::new();
            if let Some(v) = cfg.min_execution_environments {
                applied.insert("MinExecutionEnvironments".into(), json!(v));
            }
            if let Some(v) = cfg.max_execution_environments {
                applied.insert("MaxExecutionEnvironments".into(), json!(v));
            }
            let function_arn = format!(
                "arn:aws:lambda:{}:{}:function:{}",
                req.region, state.account_id, function_name
            );
            ok(json!({
                "FunctionArn": function_arn,
                "AppliedFunctionScalingConfig": Value::Object(applied.clone()),
                "RequestedFunctionScalingConfig": Value::Object(applied),
            }))
        })
    }
}
