//! `Wafv2Service` `sampled_requests` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl Wafv2Service {
    pub(super) fn get_sampled_requests(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let _web_acl_arn = require_str_len(&body, "WebAclArn", 20, 2048)?;
        let _rule_metric_name = require_str_len(&body, "RuleMetricName", 1, 255)?;
        let _scope = require_scope(&body)?;
        body.get("TimeWindow")
            .ok_or_else(|| invalid_param("TimeWindow is required"))?;
        let _max_items = require_int_range(&body, "MaxItems", 1, 500)?;
        Ok(AwsResponse::ok_json(json!({
            "SampledRequests": [],
            "PopulationSize": 0_u64,
            "TimeWindow": body.get("TimeWindow").cloned().unwrap_or(json!({})),
        })))
    }
}
