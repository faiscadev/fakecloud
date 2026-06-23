//! `LambdaService` `event_invoke` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl LambdaService {
    // ── Event invoke ──

    pub(super) fn ev_key(function: &str, qualifier: &str) -> String {
        format!("{function}:{qualifier}")
    }

    pub(super) fn put_function_event_invoke(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        let qualifier = parse_qualifier(req);
        let function_arn = format!(
            "arn:aws:lambda:{}:{}:function:{}",
            self.region_for(&req.account_id),
            req.account_id,
            function_name
        );
        // Validate Smithy ranges before persisting:
        //   MaximumEventAgeInSeconds: 60..=21600
        //   MaximumRetryAttempts:     0..=2
        // MaximumEventAgeInSeconds is optional: AWS only reports it once set.
        // Store 0 as the "unset" sentinel (0 is outside the valid 60..21600
        // range) so GetFunctionEventInvokeConfig can omit it, which the
        // Terraform resource reads back as 0.
        let event_age = body["MaximumEventAgeInSeconds"].as_i64().unwrap_or(0);
        if event_age != 0 && !(60..=21600).contains(&event_age) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                format!(
                    "MaximumEventAgeInSeconds must be 60..21600 (got {})",
                    event_age
                ),
            ));
        }
        let retries = body["MaximumRetryAttempts"].as_i64().unwrap_or(2);
        if !(0..=2).contains(&retries) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                format!("MaximumRetryAttempts must be 0..2 (got {})", retries),
            ));
        }
        let cfg = EventInvokeConfig {
            function_arn: function_arn.clone(),
            maximum_event_age: event_age,
            maximum_retry_attempts: retries,
            destination_config: body.get("DestinationConfig").cloned(),
            last_modified: Utc::now(),
        };
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .event_invoke_configs
            .insert(Self::ev_key(function_name, &qualifier), cfg.clone());
        ok(event_invoke_json(&cfg))
    }

    pub(super) fn get_function_event_invoke(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let qualifier = parse_qualifier(req);
        let region = self.region_for(&req.account_id);
        self.with_state_read(&req.account_id, &region, |state| {
            state
                .event_invoke_configs
                .get(&Self::ev_key(function_name, &qualifier))
                .map(|c| ok(event_invoke_json(c)))
                .unwrap_or_else(|| Err(not_found("EventInvokeConfig", function_name)))
        })
    }

    pub(super) fn delete_function_event_invoke(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let qualifier = parse_qualifier(req);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .event_invoke_configs
            .remove(&Self::ev_key(function_name, &qualifier));
        empty()
    }

    pub(super) fn list_function_event_invoke(
        &self,
        function_name: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let region = self.region_for(account_id);
        self.with_state_read(account_id, &region, |state| {
            let prefix = format!("{function_name}:");
            let configs: Vec<Value> = state
                .event_invoke_configs
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .map(|(_, c)| event_invoke_json(c))
                .collect();
            ok(json!({"FunctionEventInvokeConfigs": configs}))
        })
    }
}
