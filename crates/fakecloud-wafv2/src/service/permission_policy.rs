//! `Wafv2Service` `permission_policy` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl Wafv2Service {
    pub(super) fn put_permission_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resource_arn = require_str(&body, "ResourceArn")?;
        let policy = require_str(&body, "Policy")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.rule_groups.values().any(|r| r.arn == resource_arn) {
            return Err(not_found("RuleGroup"));
        }
        account.permission_policies.insert(resource_arn, policy);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn get_permission_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resource_arn = require_str(&body, "ResourceArn")?;
        let state = self.state.read();
        let policy = state
            .accounts
            .get(&req.account_id)
            .and_then(|a| a.permission_policies.get(&resource_arn))
            .cloned()
            .ok_or_else(|| not_found("PermissionPolicy"))?;
        Ok(AwsResponse::ok_json(json!({ "Policy": policy })))
    }

    pub(super) fn delete_permission_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resource_arn = require_str_len(&body, "ResourceArn", 20, 2048)?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        account.permission_policies.remove(&resource_arn);
        Ok(AwsResponse::ok_json(json!({})))
    }
}
