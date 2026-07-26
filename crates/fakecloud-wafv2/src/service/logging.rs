//! `Wafv2Service` `logging` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl Wafv2Service {
    pub(super) fn put_logging_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let cfg = body
            .get("LoggingConfiguration")
            .cloned()
            .ok_or_else(|| invalid_param("LoggingConfiguration is required"))?;
        let acl_arn = cfg
            .get("ResourceArn")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| invalid_param("LoggingConfiguration.ResourceArn is required"))?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.web_acls.values().any(|a| a.arn == acl_arn) {
            return Err(not_found("WebACL"));
        }
        account.logging_configs.insert(acl_arn, cfg.clone());
        Ok(AwsResponse::ok_json(json!({
            "LoggingConfiguration": cfg,
        })))
    }

    pub(super) fn get_logging_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let acl_arn = require_str(&body, "ResourceArn")?;
        let state = self.state.read();
        let cfg = state
            .accounts
            .get(&req.account_id)
            .and_then(|a| a.logging_configs.get(&acl_arn))
            .cloned()
            .ok_or_else(|| not_found("LoggingConfiguration"))?;
        Ok(AwsResponse::ok_json(json!({
            "LoggingConfiguration": cfg,
        })))
    }

    pub(super) fn delete_logging_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let acl_arn = require_str(&body, "ResourceArn")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account.logging_configs.remove(&acl_arn).is_none() {
            return Err(not_found("LoggingConfiguration"));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn list_logging_configurations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let scope = require_scope(&body)?;
        validate_opt_limit(&body)?;
        validate_opt_next_marker(&body)?;
        if let Some(ls) = body.get("LogScope").and_then(Value::as_str) {
            validate_enum(
                ls,
                &[
                    "CUSTOMER",
                    "SECURITY_LAKE",
                    "CLOUDWATCH_TELEMETRY_RULE_MANAGED",
                ],
                "LogScope",
            )?;
        }
        let limit = body.get("Limit").and_then(Value::as_u64).unwrap_or(100) as usize;
        let next_marker = body.get("NextMarker").and_then(Value::as_str).unwrap_or("");
        let state = self.state.read();
        let mut all: Vec<Value> = state
            .accounts
            .get(&req.account_id)
            .map(|a| {
                a.logging_configs
                    .values()
                    .filter(|cfg| {
                        let arn = cfg.get("ResourceArn").and_then(Value::as_str).unwrap_or("");
                        a.web_acls
                            .values()
                            .any(|w| w.arn == arn && w.scope == scope)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        all.sort_by(|a, b| {
            let a_arn = a.get("ResourceArn").and_then(Value::as_str).unwrap_or("");
            let b_arn = b.get("ResourceArn").and_then(Value::as_str).unwrap_or("");
            a_arn.cmp(b_arn)
        });
        // Configs are sorted by ResourceArn and the marker is the last
        // ResourceArn of the previous page, so resume at the first config
        // strictly greater than the marker. A strict `>` keeps the pager moving
        // forward even when the marked config was deleted between pages (a plain
        // "find marker, start after" would restart at page 1).
        let start = if next_marker.is_empty() {
            0
        } else {
            all.iter()
                .position(|c| {
                    c.get("ResourceArn").and_then(Value::as_str).unwrap_or("") > next_marker
                })
                .unwrap_or(all.len())
        };
        let page: Vec<Value> = all.iter().skip(start).take(limit).cloned().collect();
        let next = if start + page.len() < all.len() {
            page.last()
                .and_then(|c| c.get("ResourceArn"))
                .and_then(Value::as_str)
                .map(|s| s.to_string())
        } else {
            None
        };
        let mut out = json!({ "LoggingConfigurations": page });
        if let Some(n) = next {
            out["NextMarker"] = json!(n);
        }
        Ok(AwsResponse::ok_json(out))
    }
}
