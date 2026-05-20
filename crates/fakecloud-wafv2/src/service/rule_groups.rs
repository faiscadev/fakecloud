//! `Wafv2Service` `rule_groups` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl Wafv2Service {
    pub(super) fn create_rule_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str_len(&body, "Name", 1, 128)?;
        let scope = require_scope(&body)?;
        let capacity = body
            .get("Capacity")
            .and_then(Value::as_i64)
            .ok_or_else(|| invalid_param("Capacity is required"))?;
        let visibility_config = body
            .get("VisibilityConfig")
            .cloned()
            .ok_or_else(|| invalid_param("VisibilityConfig is required"))?;
        let rules = body
            .get("Rules")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let custom_response_bodies = parse_custom_response_bodies(body.get("CustomResponseBodies"));
        let available_labels = body
            .get("AvailableLabels")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let consumed_labels = body
            .get("ConsumedLabels")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let tags = parse_tags(body.get("Tags"))?;

        let used = compute_capacity(&rules);
        if used > capacity {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "WAFLimitsExceededException",
                format!("Rules consume {used} WCU but capacity is {capacity}"),
            ));
        }

        let key = (scope.clone(), name.clone());
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account.rule_groups.contains_key(&key) {
            return Err(already_exists(&format!("RuleGroup {name} already exists")));
        }
        let id = synth_uuid();
        let arn = synth_arn(
            &req.account_id,
            &req.region,
            &scope,
            "rulegroup",
            &name,
            &id,
        );
        let lock_token = synth_uuid();
        let label_namespace = format!("awswaf:{}:rulegroup:{name}:", req.account_id);
        let summary =
            rule_group_summary_json(&id, &name, &arn, description.as_deref(), &lock_token);
        let rg = RuleGroup {
            id,
            name,
            arn: arn.clone(),
            scope: scope.clone(),
            capacity,
            description,
            rules,
            visibility_config,
            lock_token,
            label_namespace,
            custom_response_bodies,
            available_labels,
            consumed_labels,
            created_time: Utc::now(),
        };
        account.rule_groups.insert(key, rg);
        if !tags.is_empty() {
            account.tags.insert(arn, tags);
        }
        Ok(AwsResponse::ok_json(json!({ "Summary": summary })))
    }

    pub(super) fn get_rule_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn_in = body.get("ARN").and_then(Value::as_str).map(str::to_owned);
        let state = self.state.read();
        let account = state
            .accounts
            .get(&req.account_id)
            .ok_or_else(|| not_found("RuleGroup"))?;
        let rg = if let Some(arn) = arn_in.as_deref() {
            account
                .rule_groups
                .values()
                .find(|r| r.arn == arn)
                .ok_or_else(|| not_found("RuleGroup"))?
        } else {
            let name = require_str(&body, "Name")?;
            let scope = require_scope(&body)?;
            account
                .rule_groups
                .get(&(scope, name))
                .ok_or_else(|| not_found("RuleGroup"))?
        };
        Ok(AwsResponse::ok_json(json!({
            "RuleGroup": rule_group_detail_json(rg),
            "LockToken": rg.lock_token,
        })))
    }

    pub(super) fn list_rule_groups(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let scope = require_scope(&body)?;
        validate_opt_limit(&body)?;
        validate_opt_next_marker(&body)?;
        let limit = body.get("Limit").and_then(Value::as_u64).unwrap_or(100) as usize;
        let next_marker = body
            .get("NextMarker")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let state = self.state.read();
        let mut all: Vec<RuleGroup> = state
            .accounts
            .get(&req.account_id)
            .map(|a| {
                a.rule_groups
                    .values()
                    .filter(|x| x.scope == scope)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        let (page, next) = paginate(&all, next_marker.as_deref(), limit);
        let summaries: Vec<Value> = page
            .iter()
            .map(|r| {
                rule_group_summary_json(
                    &r.id,
                    &r.name,
                    &r.arn,
                    r.description.as_deref(),
                    &r.lock_token,
                )
            })
            .collect();
        let mut response = json!({ "RuleGroups": summaries });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextMarker".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn update_rule_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "Name")?;
        let scope = require_scope(&body)?;
        let id_in = require_str(&body, "Id")?;
        let lock_token_in = require_str(&body, "LockToken")?;
        let visibility_config = body
            .get("VisibilityConfig")
            .cloned()
            .ok_or_else(|| invalid_param("VisibilityConfig is required"))?;
        let rules = body
            .get("Rules")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let rg = account
            .rule_groups
            .get_mut(&(scope, name.clone()))
            .ok_or_else(|| not_found("RuleGroup"))?;
        if rg.id != id_in {
            return Err(invalid_param("Id does not match the named RuleGroup"));
        }
        if rg.lock_token != lock_token_in {
            return Err(stale_lock_token());
        }
        let used = compute_capacity(&rules);
        if used > rg.capacity {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "WAFLimitsExceededException",
                format!("Rules consume {used} WCU but capacity is {}", rg.capacity),
            ));
        }
        rg.visibility_config = visibility_config;
        rg.rules = rules;
        rg.description = description;
        if let Some(b) = body.get("CustomResponseBodies") {
            rg.custom_response_bodies = parse_custom_response_bodies(Some(b));
        }
        rg.lock_token = synth_uuid();
        Ok(AwsResponse::ok_json(
            json!({ "NextLockToken": rg.lock_token }),
        ))
    }

    pub(super) fn delete_rule_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "Name")?;
        let scope = require_scope(&body)?;
        let id_in = require_str(&body, "Id")?;
        let lock_token_in = require_str(&body, "LockToken")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let key = (scope, name);
        let rg = account
            .rule_groups
            .get(&key)
            .ok_or_else(|| not_found("RuleGroup"))?;
        if rg.id != id_in {
            return Err(invalid_param("Id does not match the named RuleGroup"));
        }
        if rg.lock_token != lock_token_in {
            return Err(stale_lock_token());
        }
        let arn = rg.arn.clone();
        // Reject if any web ACL still references the rule group.
        let referenced = account.web_acls.values().any(|acl| {
            acl.rules.iter().any(|rule| {
                rule.get("Statement")
                    .and_then(|s| s.get("RuleGroupReferenceStatement"))
                    .and_then(|s| s.get("ARN"))
                    .and_then(Value::as_str)
                    == Some(arn.as_str())
            })
        });
        if referenced {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "WAFAssociatedItemException",
                "RuleGroup is referenced by one or more WebACLs",
            ));
        }
        account.rule_groups.remove(&key);
        account.tags.remove(&arn);
        account.permission_policies.remove(&arn);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn describe_all_managed_products(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let _scope = require_scope(&body)?;
        Ok(AwsResponse::ok_json(json!({
            "ManagedProducts": managed_products(),
        })))
    }

    pub(super) fn describe_managed_products_by_vendor(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let vendor = require_str_len(&body, "VendorName", 1, 128)?;
        let _scope = require_scope(&body)?;
        let products: Vec<Value> = managed_products()
            .into_iter()
            .filter(|p| p.get("VendorName").and_then(Value::as_str) == Some(vendor.as_str()))
            .collect();
        Ok(AwsResponse::ok_json(json!({
            "ManagedProducts": products,
        })))
    }

    pub(super) fn describe_managed_rule_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let vendor = require_str_len(&body, "VendorName", 1, 128)?;
        let name = require_str_len(&body, "Name", 1, 128)?;
        let _scope = require_scope(&body)?;
        let version =
            opt_str_len(&body, "VersionName", 1, 64)?.unwrap_or_else(|| "Version_1.0".to_string());
        Ok(AwsResponse::ok_json(json!({
            "VersionName": version,
            "SnsTopicArn": Arn::new("sns", "us-east-1", "", &format!("{vendor}-{name}-notifications")).to_string(),
            "Capacity": 50,
            "Rules": managed_rule_summaries(&vendor, &name),
            "LabelNamespace": format!("awswaf:managed:{vendor}:{name}:"),
            "AvailableLabels": [],
            "ConsumedLabels": [],
        })))
    }

    pub(super) fn get_managed_rule_set(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str_len(&body, "Name", 1, 128)?;
        let id = require_str_len(&body, "Id", 1, 36)?;
        let _scope = require_scope(&body)?;
        Ok(AwsResponse::ok_json(json!({
            "ManagedRuleSet": {
                "Name": name,
                "Id": id,
                "ARN": Arn::new("wafv2", &req.region, &req.account_id, &format!("managedruleset/{name}/{id}")).to_string(),
                "Description": format!("Managed rule set {name}"),
                "PublishedVersions": {
                    "Version_1.0": {
                        "AssociatedRuleGroupArn": Arn::new("wafv2", &req.region, "aws", &format!("managedrulegroup/{name}")).to_string(),
                        "Capacity": 50,
                        "ForecastedLifetime": 90,
                        "PublishTimestamp": Utc::now().timestamp() as f64,
                        "LastUpdateTimestamp": Utc::now().timestamp() as f64,
                        "ExpiryTimestamp": (Utc::now() + chrono::Duration::days(365)).timestamp() as f64,
                    }
                },
                "RecommendedVersion": "Version_1.0",
                "LabelNamespace": format!("awswaf:managed::{name}:"),
            },
            "LockToken": synth_uuid(),
        })))
    }

    pub(super) fn list_available_managed_rule_groups(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let _scope = require_scope(&body)?;
        validate_opt_limit(&body)?;
        validate_opt_next_marker(&body)?;
        Ok(AwsResponse::ok_json(json!({
            "ManagedRuleGroups": [
                {
                    "VendorName": "AWS",
                    "Name": "AWSManagedRulesCommonRuleSet",
                    "VersioningSupported": true,
                    "Description": "OWASP Top 10 baseline rules",
                },
                {
                    "VendorName": "AWS",
                    "Name": "AWSManagedRulesKnownBadInputsRuleSet",
                    "VersioningSupported": true,
                    "Description": "Block request patterns associated with known exploits",
                },
                {
                    "VendorName": "AWS",
                    "Name": "AWSManagedRulesSQLiRuleSet",
                    "VersioningSupported": true,
                    "Description": "SQL injection patterns",
                },
            ],
        })))
    }

    pub(super) fn list_available_managed_rule_group_versions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let _vendor = require_str_len(&body, "VendorName", 1, 128)?;
        let _name = require_str_len(&body, "Name", 1, 128)?;
        let _scope = require_scope(&body)?;
        validate_opt_limit(&body)?;
        validate_opt_next_marker(&body)?;
        Ok(AwsResponse::ok_json(json!({
            "Versions": [
                {"Name": "Version_1.0", "LastUpdateTimestamp": Utc::now().timestamp() as f64},
                {"Name": "Version_2.0", "LastUpdateTimestamp": Utc::now().timestamp() as f64},
            ],
            "CurrentDefaultVersion": "Version_2.0",
        })))
    }

    pub(super) fn list_managed_rule_sets(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let _scope = require_scope(&body)?;
        validate_opt_limit(&body)?;
        validate_opt_next_marker(&body)?;
        // fakecloud does not expose vendor-side managed rule set publishing,
        // so this always returns an empty list (matches accounts without
        // FirewallManager publishing rights).
        Ok(AwsResponse::ok_json(json!({ "ManagedRuleSets": [] })))
    }

    pub(super) fn put_managed_rule_set_versions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "Name")?;
        fakecloud_core::validation::validate_string_length("Name", &name, 1, 128)?;
        let id = require_str(&body, "Id")?;
        fakecloud_core::validation::validate_string_length("Id", &id, 1, 36)?;
        let lock_token = require_str(&body, "LockToken")?;
        fakecloud_core::validation::validate_string_length("LockToken", &lock_token, 1, 36)?;
        let _scope = require_scope(&body)?;
        if let Some(recommended_version) = body.get("RecommendedVersion").and_then(Value::as_str) {
            fakecloud_core::validation::validate_string_length(
                "RecommendedVersion",
                recommended_version,
                1,
                64,
            )?;
        }
        Ok(AwsResponse::ok_json(json!({
            "NextLockToken": synth_uuid(),
        })))
    }

    pub(super) fn update_managed_rule_set_version_expiry_date(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "Name")?;
        fakecloud_core::validation::validate_string_length("Name", &name, 1, 128)?;
        let id = require_str(&body, "Id")?;
        fakecloud_core::validation::validate_string_length("Id", &id, 1, 36)?;
        let lock_token = require_str(&body, "LockToken")?;
        fakecloud_core::validation::validate_string_length("LockToken", &lock_token, 1, 36)?;
        let _scope = require_scope(&body)?;
        let version_to_expire = require_str(&body, "VersionToExpire")?;
        fakecloud_core::validation::validate_string_length(
            "VersionToExpire",
            &version_to_expire,
            1,
            64,
        )?;
        let expiry_timestamp = body
            .get("ExpiryTimestamp")
            .and_then(Value::as_f64)
            .ok_or_else(|| invalid_param("ExpiryTimestamp is required"))?;
        Ok(AwsResponse::ok_json(json!({
            "ExpiringVersion": version_to_expire,
            "ExpiryTimestamp": expiry_timestamp,
            "NextLockToken": synth_uuid(),
        })))
    }

    pub(super) fn delete_firewall_manager_rule_groups(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let acl_arn = require_str(&body, "WebACLArn")?;
        let _lock_token = require_str(&body, "WebACLLockToken")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let acl = account
            .web_acls
            .values_mut()
            .find(|a| a.arn == acl_arn)
            .ok_or_else(|| not_found("WebACL"))?;
        acl.pre_process_firewall_manager_rule_groups.clear();
        acl.post_process_firewall_manager_rule_groups.clear();
        acl.lock_token = synth_uuid();
        Ok(AwsResponse::ok_json(json!({
            "NextWebACLLockToken": acl.lock_token,
        })))
    }
}
