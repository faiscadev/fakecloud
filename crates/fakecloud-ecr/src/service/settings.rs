//! `EcrService` `settings` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl EcrService {
    pub(super) fn get_account_setting(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "name")?.to_string();
        validate_account_setting_name(&name)?;
        let account = target_account_id(request, &body);
        let accounts = self.state.read();
        let state = accounts.get(&account);
        let value = state
            .and_then(|s| s.account_settings.get(&name).cloned())
            .unwrap_or_else(|| "DISABLED".to_string());
        Ok(AwsResponse::ok_json(json!({
            "name": name,
            "value": value,
        })))
    }

    pub(super) fn put_account_setting(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "name")?.to_string();
        validate_account_setting_name(&name)?;
        let value = req_str(&body, "value")?.to_string();
        let account = target_account_id(request, &body);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        state.account_settings.insert(name.clone(), value.clone());
        Ok(AwsResponse::ok_json(json!({
            "name": name,
            "value": value,
        })))
    }
}
