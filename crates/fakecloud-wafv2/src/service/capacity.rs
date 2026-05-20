//! `Wafv2Service` `capacity` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl Wafv2Service {
    pub(super) fn check_capacity(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let _scope = require_scope(&body)?;
        // Rules is @required; must be present (even if empty array is allowed
        // semantically in some shapes, the field itself must be supplied).
        let rules = body
            .get("Rules")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| invalid_param("Rules is required"))?;
        Ok(AwsResponse::ok_json(json!({
            "Capacity": compute_capacity(&rules),
        })))
    }

    pub(super) fn get_rate_based_statement_managed_keys(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let _scope = require_scope(&body)?;
        let _web_acl_name = require_str_len(&body, "WebACLName", 1, 128)?;
        let _web_acl_id = require_str_len(&body, "WebACLId", 1, 36)?;
        let _rule_name = require_str_len(&body, "RuleName", 1, 128)?;
        opt_str_len(&body, "RuleGroupRuleName", 1, 128)?;
        Ok(AwsResponse::ok_json(json!({
            "ManagedKeysIPV4": {"IPAddressVersion": "IPV4", "Addresses": []},
            "ManagedKeysIPV6": {"IPAddressVersion": "IPV6", "Addresses": []},
        })))
    }
}
