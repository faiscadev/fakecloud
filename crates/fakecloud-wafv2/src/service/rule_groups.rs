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
        let (page, next) = paginate_checked(&all, next_marker.as_deref(), limit)
            .map_err(|_| invalid_param("The specified NextMarker is not valid."))?;
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
        // Update is full-replace: an omitted CustomResponseBodies clears it.
        rg.custom_response_bodies = parse_custom_response_bodies(body.get("CustomResponseBodies"));
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
        // AWS rejects an unknown (vendor, name) with WAFInvalidParameterException
        // rather than returning a fabricated rule group.
        let def = managed_group_def(&vendor, &name).ok_or_else(|| {
            invalid_param(format!(
                "The managed rule group {vendor}/{name} does not exist."
            ))
        })?;
        let available_labels: Vec<Value> = def
            .rules
            .iter()
            .map(|r| json!({ "Name": format!("awswaf:managed:{vendor}:{name}:{r}") }))
            .collect();
        Ok(AwsResponse::ok_json(json!({
            "VersionName": version,
            "SnsTopicArn": Arn::new("sns", "us-east-1", "", &format!("{vendor}-{name}-notifications")).to_string(),
            "Capacity": def.capacity,
            "Rules": managed_rule_summaries(&vendor, &name),
            "LabelNamespace": format!("awswaf:managed:{vendor}:{name}:"),
            "AvailableLabels": available_labels,
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
        let scope = require_scope(&body)?;
        // GetManagedRuleSet reads a set previously published via
        // PutManagedRuleSetVersions. AWS returns WAFNonexistentItemException
        // for an unknown (scope, name) or a mismatched Id.
        let state = self.state.read();
        let set = state
            .accounts
            .get(&req.account_id)
            .and_then(|a| a.managed_rule_sets.get(&(scope, name.clone())))
            .filter(|s| s.id == id)
            .ok_or_else(|| not_found("ManagedRuleSet"))?;

        // Build PublishedVersions from the authoritative version-name list,
        // using the per-version detail when present. Snapshots written before
        // published_version_details existed only carry the names, so synthesize
        // a minimal detail for those rather than dropping the version.
        let mut published = serde_json::Map::new();
        for v in &set.published_versions {
            let detail = set
                .published_version_details
                .get(v)
                .cloned()
                .unwrap_or_else(|| json!({ "Capacity": 50 }));
            published.insert(v.clone(), detail);
        }
        // Include any detail-only entries (defensive) not in the name list.
        for (v, d) in &set.published_version_details {
            published.entry(v.clone()).or_insert_with(|| d.clone());
        }
        let mut managed = serde_json::Map::new();
        managed.insert("Name".to_string(), json!(set.name));
        managed.insert("Id".to_string(), json!(set.id));
        managed.insert(
            "ARN".to_string(),
            json!(Arn::new(
                "wafv2",
                &req.region,
                &req.account_id,
                &format!("managedruleset/{}/{}", set.name, set.id)
            )
            .to_string()),
        );
        if let Some(d) = &set.description {
            managed.insert("Description".to_string(), json!(d));
        }
        managed.insert("PublishedVersions".to_string(), Value::Object(published));
        if let Some(rv) = &set.recommended_version {
            managed.insert("RecommendedVersion".to_string(), json!(rv));
        }
        managed.insert("LabelNamespace".to_string(), json!(set.label_namespace));
        Ok(AwsResponse::ok_json(json!({
            "ManagedRuleSet": Value::Object(managed),
            "LockToken": set.lock_token,
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
        let groups: Vec<Value> = managed_rule_group_catalog()
            .iter()
            .map(|d| {
                json!({
                    "VendorName": d.vendor,
                    "Name": d.name,
                    "VersioningSupported": true,
                    "Description": d.description,
                })
            })
            .collect();
        Ok(AwsResponse::ok_json(json!({
            "ManagedRuleGroups": groups,
        })))
    }

    pub(super) fn list_available_managed_rule_group_versions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let _vendor = require_str_len(&body, "VendorName", 1, 128)?;
        let name = require_str_len(&body, "Name", 1, 128)?;
        let scope = require_scope(&body)?;
        validate_opt_limit(&body)?;
        validate_opt_next_marker(&body)?;

        // Return the versions actually published via
        // PutManagedRuleSetVersions for this (scope, name) if any exist;
        // otherwise fall back to the documented AWS-vendor sample set.
        let state = self.state.read();
        if let Some(set) = state
            .accounts
            .get(&req.account_id)
            .and_then(|a| a.managed_rule_sets.get(&(scope.clone(), name.clone())))
        {
            let versions: Vec<Value> = set
                .published_versions
                .iter()
                .map(|v| json!({"Name": v, "LastUpdateTimestamp": set.created_time.timestamp() as f64}))
                .collect();
            let current = set
                .recommended_version
                .clone()
                .or_else(|| set.published_versions.last().cloned());
            return Ok(AwsResponse::ok_json(json!({
                "Versions": versions,
                "CurrentDefaultVersion": current,
            })));
        }

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
        let scope = require_scope(&body)?;
        validate_opt_limit(&body)?;
        validate_opt_next_marker(&body)?;

        // Return the managed rule sets this account has published for the
        // requested scope (via PutManagedRuleSetVersions).
        let state = self.state.read();
        let sets: Vec<Value> = state
            .accounts
            .get(&req.account_id)
            .map(|a| {
                a.managed_rule_sets
                    .values()
                    .filter(|s| s.scope == scope)
                    .map(|s| {
                        json!({
                            "Name": s.name,
                            "Id": s.id,
                            "Description": s.description,
                            "LockToken": s.lock_token,
                            "LabelNamespace": s.label_namespace,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "ManagedRuleSets": sets })))
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
        let scope = require_scope(&body)?;
        let recommended_version = match body.get("RecommendedVersion").and_then(Value::as_str) {
            Some(v) => {
                fakecloud_core::validation::validate_string_length("RecommendedVersion", v, 1, 64)?;
                Some(v.to_string())
            }
            None => None,
        };

        // VersionsToPublish is a map of version-name ->
        // {AssociatedRuleGroupArn, ForecastedLifetime}. Persist both the
        // version names and their detail so GetManagedRuleSet can read the
        // full PublishedVersions back.
        let versions_to_publish = body
            .get("VersionsToPublish")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();

        let next_lock_token = synth_uuid();
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let key = (scope.clone(), name.clone());
        let entry =
            account
                .managed_rule_sets
                .entry(key)
                .or_insert_with(|| crate::state::ManagedRuleSet {
                    id: id.clone(),
                    name: name.clone(),
                    scope: scope.clone(),
                    description: None,
                    lock_token: next_lock_token.clone(),
                    label_namespace: format!("awswaf:managed:{name}"),
                    recommended_version: None,
                    published_versions: Vec::new(),
                    published_version_details: std::collections::BTreeMap::new(),
                    created_time: Utc::now(),
                });
        let now_ts = Utc::now().timestamp() as f64;
        for (vname, cfg) in &versions_to_publish {
            if !entry.published_versions.contains(vname) {
                entry.published_versions.push(vname.clone());
            }
            let mut detail = serde_json::Map::new();
            if let Some(arn) = cfg.get("AssociatedRuleGroupArn") {
                detail.insert("AssociatedRuleGroupArn".to_string(), arn.clone());
            }
            if let Some(fl) = cfg.get("ForecastedLifetime") {
                detail.insert("ForecastedLifetime".to_string(), fl.clone());
            }
            detail.insert(
                "Capacity".to_string(),
                json!(cfg.get("Capacity").and_then(Value::as_i64).unwrap_or(50)),
            );
            // Preserve the original PublishTimestamp when republishing an
            // existing version; only LastUpdateTimestamp advances.
            let publish_ts = entry
                .published_version_details
                .get(vname)
                .and_then(|d| d.get("PublishTimestamp"))
                .and_then(Value::as_f64)
                .unwrap_or(now_ts);
            detail.insert("PublishTimestamp".to_string(), json!(publish_ts));
            detail.insert("LastUpdateTimestamp".to_string(), json!(now_ts));
            entry
                .published_version_details
                .insert(vname.clone(), Value::Object(detail));
        }
        entry.published_versions.sort();
        if recommended_version.is_some() {
            entry.recommended_version = recommended_version;
        }
        entry.lock_token = next_lock_token.clone();

        Ok(AwsResponse::ok_json(json!({
            "NextLockToken": next_lock_token,
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
        let scope = require_scope(&body)?;
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

        // When the managed rule set (scope, name, matching Id) and the named
        // published version exist, persist the expiry onto that version so a
        // later GetManagedRuleSet surfaces it (previously this validated and
        // returned Ok without writing anything). A request against a set/version
        // that was never created still returns success, matching AWS's smoke
        // response for the op rather than adding a hard dependency on prior
        // state.
        let next_lock_token = synth_uuid();
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if let Some(set) = account
            .managed_rule_sets
            .get_mut(&(scope, name.clone()))
            .filter(|s| s.id == id)
        {
            if set.published_versions.contains(&version_to_expire) {
                let detail = set
                    .published_version_details
                    .entry(version_to_expire.clone())
                    .or_insert_with(|| json!({ "Capacity": 50 }));
                if let Some(obj) = detail.as_object_mut() {
                    obj.insert("ExpiryTimestamp".to_string(), json!(expiry_timestamp));
                }
            }
            set.lock_token = next_lock_token.clone();
        }

        Ok(AwsResponse::ok_json(json!({
            "ExpiringVersion": version_to_expire,
            "ExpiryTimestamp": expiry_timestamp,
            "NextLockToken": next_lock_token,
        })))
    }

    pub(super) fn delete_firewall_manager_rule_groups(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let acl_arn = require_str(&body, "WebACLArn")?;
        let lock_token = require_str(&body, "WebACLLockToken")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let acl = account
            .web_acls
            .values_mut()
            .find(|a| a.arn == acl_arn)
            .ok_or_else(|| not_found("WebACL"))?;
        if acl.lock_token != lock_token {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "WAFOptimisticLockException",
                "Lock token stale; refetch the WebACL and retry",
            ));
        }
        acl.pre_process_firewall_manager_rule_groups.clear();
        acl.post_process_firewall_manager_rule_groups.clear();
        acl.lock_token = synth_uuid();
        Ok(AwsResponse::ok_json(json!({
            "NextWebACLLockToken": acl.lock_token,
        })))
    }
}
