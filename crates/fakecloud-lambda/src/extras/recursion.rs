//! `LambdaService` `recursion` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl LambdaService {
    // ── Recursion ──

    pub(super) fn put_recursion_config(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        // `RecursiveLoop` is `@required` on the model — reject missing
        // values instead of defaulting silently to `Terminate`. The
        // enum admits only `Allow` and `Terminate`.
        let mode = body["RecursiveLoop"]
            .as_str()
            .ok_or_else(|| missing("RecursiveLoop"))?
            .to_string();
        if mode != "Allow" && mode != "Terminate" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                format!(
                    "Invalid RecursiveLoop value '{}'; expected 'Allow' or 'Terminate'",
                    mode
                ),
            ));
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .recursion_configs
            .insert(function_name.to_string(), mode.clone());
        ok(json!({"RecursiveLoop": mode}))
    }

    pub(super) fn get_recursion_config(
        &self,
        function_name: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let region = self.region_for(account_id);
        self.with_state_read(account_id, &region, |state| {
            let mode = state
                .recursion_configs
                .get(function_name)
                .cloned()
                .unwrap_or_else(|| "Terminate".to_string());
            ok(json!({"RecursiveLoop": mode}))
        })
    }
}
